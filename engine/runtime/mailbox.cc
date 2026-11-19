// 按队列预算接收消息、合并连续输入，并按来源配额取出待处理项。
// 关停时为尚在队列中的控制请求返回取消结果。

#include "engine/runtime/mailbox.h"

#include <algorithm>
#include <unordered_map>
#include <utility>

namespace pop {
namespace {
// 根据消息实际携带的值选择队列，Inspect 使用控制通道。
Lane LaneOf(const Payload& payload) {
  if (std::holds_alternative<ControlInput>(payload)) return Lane::Input;
  if (std::holds_alternative<WorkResult>(payload)) return Lane::Result;
  if (std::holds_alternative<Work>(payload)) return Lane::Background;
  return Lane::Control;
}
// 队列年龄从消息创建时算起；连续输入还受其自身有效期限制。
bool Expired(const Message& message, Milliseconds age, TimePoint now) {
  auto* input = std::get_if<ControlInput>(&message.m_payload);
  return now - message.m_created >= age ||
         (input && now >= input->m_validUntil);
}
}  // namespace

const char* Name(Admission admission) {
  switch (admission) {
    case Admission::Accepted:
      return "accepted";
    case Admission::Merged:
      return "merged";
    case Admission::Full:
      return "full";
    case Admission::Stale:
      return "stale";
    case Admission::Expired:
      return "expired";
    case Admission::Invalid:
      return "invalid";
    case Admission::Closed:
      return "closed";
    case Admission::UnknownSpace:
      return "unknown_space";
  }
  return "unknown";
}

WorkResult Compute(const Work& work) {
  return {work.m_key, work.m_compute(work.m_snapshot)};
}
std::size_t Message::bytes() const {
  const auto base = sizeof(Message) + m_trace.capacity();
  if (const auto* input = std::get_if<ControlInput>(&m_payload))
    return base + input->m_state.capacity();
  if (const auto* work = std::get_if<Work>(&m_payload))
    return base + work->m_snapshot.capacity();
  if (const auto* result = std::get_if<WorkResult>(&m_payload))
    return base + result->m_output.capacity();
  return base;
}

Mailbox::Mailbox(Budgets budgets) : m_budgets(budgets) {}
Admission Mailbox::post(Message message, TimePoint now) {
  // index 同时选中消息队列 items、统计 stats 和该通道的预算 budget。
  // 持锁期间一起修改队列和占用字节，保证并发投递看到一致的剩余容量。
  auto index = static_cast<std::size_t>(LaneOf(message.m_payload));
  std::lock_guard lock(m_mutex);
  auto& [items, stats] = m_queues[index];
  const auto& budget = m_budgets[index];
  // 所有拒绝分支同时累计总数与具体原因。
  auto reject = [&](Admission reason) {
    ++stats.m_rejected;
    ++stats.m_rejections[static_cast<std::size_t>(reason)];
    return reason;
  };
  if (m_closed) return reject(Admission::Closed);
  if (message.m_created > now) return reject(Admission::Invalid);
  if (Expired(message, budget.m_age, now)) return reject(Admission::Expired);
  if (const auto* bind = std::get_if<BindEntity>(&message.m_payload);
      bind && !bind->m_binding.valid())
    return reject(Admission::Invalid);
  if (const auto* work = std::get_if<Work>(&message.m_payload);
      work && !work->m_compute)
    return reject(Admission::Invalid);
  if (auto* input = std::get_if<ControlInput>(&message.m_payload)) {
    if (!input->m_binding.valid() || input->m_seq == 0)
      return reject(Admission::Invalid);
    for (auto& queued : items) {
      // prev 是待替换的控制状态；不同实体或会话绑定各自保留输入。
      const auto& prev = std::get<ControlInput>(queued.m_payload);
      if (input->m_binding != prev.m_binding) continue;
      if (input->m_seq <= prev.m_seq) return reject(Admission::Stale);
      // 扣除旧项后，other_bytes 是其余排队项仍占用的字节数。
      auto other_bytes = stats.m_bytes - queued.bytes();
      if (message.bytes() > budget.m_bytes - other_bytes)
        return reject(Admission::Full);
      // 短跟踪串替换长串时也要归还旧缓冲，保持实际队列占用与预算一致。
      queued.m_trace.swap(message.m_trace);
      queued.m_source = message.m_source;
      queued.m_created = message.m_created;
      queued.m_payload = std::move(message.m_payload);
      stats.m_bytes = other_bytes + queued.bytes();
      ++stats.m_merged;
      return Admission::Merged;
    }
  }
  if (items.size() >= budget.m_entries ||
      message.bytes() > budget.m_bytes - stats.m_bytes)
    return reject(Admission::Full);
  // 所有权移交给队列前记下消息大小，供入队后计账。
  auto bytes = message.bytes();
  items.push_back(std::move(message));
  stats.m_bytes += bytes;
  return Admission::Accepted;
}

std::vector<Delivery> Mailbox::take(Lane lane, TimePoint now,
                                    TimePoint cutoff) {
  // 用同一通道下标和同一把锁读取预算、移出消息并更新统计。
  auto index = static_cast<std::size_t>(lane);
  std::lock_guard lock(m_mutex);
  auto& [items, stats] = m_queues[index];
  const auto& budget = m_budgets[index];
  // consumed 按来源记录本批已取条数，deliveries 持有交给调用者的消息。
  std::unordered_map<Id, std::size_t> consumed;
  std::vector<Delivery> deliveries;
  for (auto it = items.begin(); it != items.end();) {
    if (it->m_created > cutoff ||
        consumed[it->m_source] >= budget.m_perSource) {
      ++it;
      continue;
    }
    // 即使已过期也交给消费者，以便控制请求能收到终态回执。
    bool expired = Expired(*it, budget.m_age, now);
    ++consumed[it->m_source];
    stats.m_expired += expired;
    stats.m_bytes -= it->bytes();
    deliveries.push_back({std::move(*it), expired});
    it = items.erase(it);
  }
  return deliveries;
}

QueueStats Mailbox::stats(Lane lane, TimePoint now) const {
  // 在持锁时给计数副本补上当前条数和最老年龄。
  std::lock_guard lock(m_mutex);
  const auto& queue = m_queues[static_cast<std::size_t>(lane)];
  auto stats = queue.m_stats;
  stats.m_entries = queue.m_items.size();
  for (const auto& item : queue.m_items) {
    stats.m_oldestAge = std::max(
        stats.m_oldestAge,
        std::chrono::duration_cast<Milliseconds>(now - item.m_created));
  }
  return stats;
}

void Mailbox::close() {
  std::lock_guard lock(m_mutex);
  m_closed = true;
}
std::size_t Mailbox::cancelRemaining() {
  std::lock_guard lock(m_mutex);
  std::size_t cancelled = 0;  // 四个通道本次实际清除的消息总数。
  for (auto& [items, stats] : m_queues) {
    cancelled += items.size();
    for (auto& item : items) {
      if (auto* request = std::get_if<Inspect>(&item.m_payload))
        request->m_reply.set_value(RequestStatus::Cancelled);
      if (auto* bind = std::get_if<BindEntity>(&item.m_payload))
        bind->m_reply.set_value(RequestStatus::Cancelled);
    }
    items.clear();
    stats.m_bytes = 0;
  }
  return cancelled;
}
}  // namespace pop
