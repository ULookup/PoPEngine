// 在发送前为 TCP 载荷加网络序前缀，按条数和字节限制在途与待写数据。
#include "engine/net/send_queue.h"

#include <algorithm>
#include <iterator>
#include <utility>

namespace pop {

SendQueue::SendQueue(NetLimits limits) : m_limits(limits) {}

void SendQueue::open(std::function<void(bool)> schedule) {
  std::lock_guard lock(m_mutex);
  m_schedule = std::move(schedule);
}

SendStatus SendQueue::push(WireBytes payload,
                           std::optional<std::uint64_t> replace_key,
                           FullQueueAction on_full) {
  std::lock_guard lock(m_mutex);
  if (!m_schedule) {
    return SendStatus::Closed;
  }
  if (payload.empty() || payload.size() > MAX_REALTIME_FRAME_BYTES) {
    return SendStatus::TooLarge;
  }

  auto replace = m_items.end();
  if (replace_key) {
    auto first = m_items.begin();
    if (m_writing) {
      ++first;
    }
    replace = std::find_if(first, m_items.end(), [&](const Item& item) {
      return item.m_key == replace_key;
    });
  }

  const auto size = STREAM_PREFIX_BYTES + payload.size();
  const auto other_bytes =
      m_bytes - (replace == m_items.end() ? 0 : replace->m_wire->size());
  if ((replace == m_items.end() && m_items.size() >= m_limits.m_queueEntries) ||
      size > m_limits.m_queueBytes - other_bytes) {
    if (!replace_key && on_full == FullQueueAction::Close) {
      auto schedule = std::move(m_schedule);
      m_schedule = {};
      schedule(true);
    }
    return SendStatus::Full;
  }

  // 按实际长度分配，调用方预留的大容量不会进入队列预算之外。
  auto frame = std::make_shared<WireBytes>(EncodeStreamFrame(payload));

  if (replace != m_items.end()) {
    // 新状态排到已入队的可靠事件之后。
    std::rotate(replace, std::next(replace), m_items.end());
    m_items.back().m_wire = std::move(frame);
    m_bytes = other_bytes + size;
    return SendStatus::Merged;
  }

  const bool idle = m_items.empty();
  m_items.push_back({replace_key, std::move(frame)});
  m_bytes += size;
  if (idle) {
    m_schedule(false);
  }
  return SendStatus::Queued;
}

std::shared_ptr<const WireBytes> SendQueue::beginWrite() {
  std::lock_guard lock(m_mutex);
  if (!m_schedule || m_writing || m_items.empty()) {
    return {};
  }
  m_writing = true;
  return m_items.front().m_wire;
}

bool SendQueue::finishWrite() {
  std::lock_guard lock(m_mutex);
  if (!m_writing) {
    return false;
  }
  m_bytes -= m_items.front().m_wire->size();
  m_items.pop_front();
  m_writing = false;
  return !m_items.empty() && static_cast<bool>(m_schedule);
}

void SendQueue::close() {
  std::lock_guard lock(m_mutex);
  m_schedule = {};
  m_items.clear();
  m_writing = false;
  m_bytes = 0;
}

}  // namespace pop
