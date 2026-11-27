// 合并同一实体绑定的待发输入，在短窗口结束时按 Space 与承载实例批量转发。
#include "engine/net/input_forwarder.h"

#include <algorithm>
#include <optional>
#include <utility>

#include "engine/core/log.h"
#include "engine/net/wire_frame.h"

namespace pop {
namespace {

// 从空队列收到第一条输入起计时，后续同窗口输入不推迟截止点。
constexpr Milliseconds BATCH_WINDOW{2};

// 实体重生或会话重新绑定后，先前一代的输入不能覆盖新绑定。
bool SameBinding(const wire::RoutedInput& a, const wire::RoutedInput& b) {
  return a.entity_id() == b.entity_id() &&
         a.entity_generation() == b.entity_generation() &&
         a.session_epoch() == b.session_epoch();
}

}  // namespace

InputForwarder::InputForwarder(TlsClient& client, NetLimits limits,
                               Refusal on_refusal)
    : m_client(client),
      m_limits(limits),
      m_onRefusal(std::move(on_refusal)),
      m_thread([this](std::stop_token stop) { run(stop); }) {}

InputForwarder::~InputForwarder() = default;

void InputForwarder::refuse(Id request, Id sequence, SpaceId space,
                            wire::InputStatus status) const {
  try {
    m_onRefusal(request, sequence, space, status);
  } catch (...) {
    LOG_ERROR("event=forward_reply_failed request={} input_seq={} space={}",
              request, sequence, space);
  }
}

void InputForwarder::post(SpaceId space, Id owner_epoch,
                          const std::string& worker, wire::RoutedInput input) {
  auto request = input.request_id();
  auto sequence = input.input_seq();
  const auto now = Clock::now();
  const auto wall_now = UnixMilliseconds();
  if (input.expires_at_unix_ms() <= wall_now) {
    refuse(request, sequence, space, wire::EXPIRED);
    return;
  }
  const auto ttl = Milliseconds(
      std::min(input.expires_at_unix_ms() - wall_now, INPUT_LIFETIME.count()));
  const auto deadline = now + ttl;
  // 同时计入 Protobuf 的动态存储、路由串以及 list 节点的两个链指针。
  const auto bytes = sizeof(Item) - sizeof(input) + input.SpaceUsedLong() +
                     worker.size() + 2 * sizeof(void*);

  std::optional<wire::InputStatus> refusal;
  {
    std::lock_guard lock(m_mutex);
    auto previous =
        std::find_if(m_pending.begin(), m_pending.end(), [&](const Item& item) {
          return item.m_space == space && item.m_ownerEpoch == owner_epoch &&
                 item.m_worker == worker && SameBinding(input, item.m_input);
        });
    const bool replacing = previous != m_pending.end();
    const auto other_bytes = m_bytes - (replacing ? previous->m_bytes : 0);
    if (replacing && sequence <= previous->m_input.input_seq()) {
      refusal = wire::STALE;
    } else if ((!replacing && m_count >= m_limits.m_queueEntries) ||
               bytes > m_limits.m_queueBytes - other_bytes ||
               bytes > MAX_REALTIME_FRAME_BYTES) {
      refusal = wire::BUSY;
    } else if (replacing) {
      request = previous->m_input.request_id();
      sequence = previous->m_input.input_seq();
      previous->m_input = std::move(input);
      previous->m_deadline = deadline;
      previous->m_bytes = bytes;
      m_bytes = other_bytes + bytes;
      refusal = wire::SUPERSEDED;
    } else {
      const bool first = m_pending.empty();
      m_pending.push_back(
          {space, owner_epoch, worker, std::move(input), deadline, bytes});
      ++m_count;
      m_bytes += bytes;
      if (first) {
        m_flushAt = now + BATCH_WINDOW;
        m_wake.notify_one();
      }
    }
  }
  if (refusal) {
    refuse(request, sequence, space, *refusal);
  }
}

void InputForwarder::run(std::stop_token stop) {
  std::unique_lock lock(m_mutex);
  while (!stop.stop_requested()) {
    m_wake.wait(lock, stop, [this] { return !m_pending.empty(); });
    if (stop.stop_requested()) {
      break;
    }
    m_wake.wait_until(lock, stop, m_flushAt, [] { return false; });
    if (stop.stop_requested()) {
      break;
    }

    const auto& route = m_pending.front();
    std::list<Item> batch;
    std::size_t batch_bytes = 0;
    for (auto it = m_pending.begin(); it != m_pending.end();) {
      auto next = it++;
      if (next->m_space != route.m_space ||
          next->m_ownerEpoch != route.m_ownerEpoch ||
          next->m_worker != route.m_worker) {
        continue;
      }
      if (batch.size() == MAX_INPUTS_PER_BATCH ||
          next->m_bytes > MAX_REALTIME_FRAME_BYTES - batch_bytes) {
        break;
      }
      batch_bytes += next->m_bytes;
      batch.splice(batch.end(), m_pending, next);
    }

    const auto count = batch.size();
    lock.unlock();
    dispatch(batch);
    lock.lock();
    m_count -= count;
    m_bytes -= batch_bytes;
  }

  auto cancelled = std::move(m_pending);
  lock.unlock();
  for (const auto& item : cancelled) {
    refuse(item.m_input.request_id(), item.m_input.input_seq(), item.m_space,
           wire::REJECTED);
  }
}

void InputForwarder::dispatch(std::list<Item>& items) {
  const auto now = Clock::now();
  items.remove_if([&](const Item& item) {
    if (item.m_deadline > now) {
      return false;
    }
    refuse(item.m_input.request_id(), item.m_input.input_seq(), item.m_space,
           wire::EXPIRED);
    return true;
  });
  if (items.empty()) {
    return;
  }

  bool attempted = false;
  auto status = wire::BUSY;
  try {
    const auto& target = items.front();
    wire::InputBatch batch;
    batch.set_space_id(target.m_space);
    batch.set_space_owner_epoch(target.m_ownerEpoch);
    batch.set_worker_instance_id(target.m_worker);
    for (const auto& item : items) {
      *batch.add_inputs() = item.m_input;
    }
    auto frame = EncodeFrame(wire::INPUT_BATCH, batch.SerializeAsString());
    attempted = true;
    if (m_client.send(std::move(frame)) == SendStatus::Queued) {
      return;
    }
  } catch (...) {
    // 传输层异常可能发生在接管缓冲后，已知的拒绝才可明确回报 BUSY。
    status = attempted ? wire::UNKNOWN : wire::BUSY;
  }
  for (const auto& item : items) {
    refuse(item.m_input.request_id(), item.m_input.input_seq(), item.m_space,
           status);
  }
}

}  // namespace pop
