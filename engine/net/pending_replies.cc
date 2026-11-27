// 在提交和归还回送路径时串行核对关联号；所有网络回调都在表锁之外执行。
#include "engine/net/pending_replies.h"

#include <limits>
#include <utility>

#include "engine/core/log.h"

namespace pop {

PendingReplies::PendingReplies(NetLimits limits, ReplyTimeout& on_timeout)
    : m_limits(limits),
      m_onTimeout(on_timeout),
      m_timer([this](std::stop_token stop) { expire(stop); }) {}

Id PendingReplies::reserve(Id client_id, SpaceId space, const SendFrame& send) {
  std::lock_guard lock(m_mutex);
  if (m_entries.size() >= m_limits.m_queueEntries ||
      m_next == std::numeric_limits<Id>::max()) {
    return 0;
  }
  const auto request = ++m_next;
  m_entries.emplace(request, Entry{{client_id, space, send},
                                   Clock::now() + m_limits.m_timeout});
  m_changed.notify_one();
  return request;
}

std::optional<ReplyRoute> PendingReplies::take(
    Id request, SpaceId space, std::optional<Id> expected_client) {
  std::unique_lock lock(m_mutex);
  auto it = m_entries.find(request);
  if (it == m_entries.end() || it->second.m_route.m_space != space ||
      (expected_client && *expected_client != it->second.m_route.m_clientId)) {
    return {};
  }
  const bool expired = Clock::now() >= it->second.m_deadline;
  auto route = std::move(it->second.m_route);
  m_entries.erase(it);
  lock.unlock();
  m_changed.notify_one();

  if (expired) {
    reportTimeout(route, request);
    return {};
  }
  return route;
}

void PendingReplies::reportTimeout(const ReplyRoute& route,
                                   Id request) const noexcept {
  try {
    m_onTimeout(route, request);
  } catch (...) {
    LOG_ERROR("event=timeout_reply_failed request={} space={}", request,
              route.m_space);
  }
}

void PendingReplies::expire(std::stop_token stop) {
  std::unique_lock lock(m_mutex);
  while (!stop.stop_requested()) {
    if (m_entries.empty()) {
      m_changed.wait(lock, stop, [this] { return !m_entries.empty(); });
      continue;
    }
    const auto it = m_entries.begin();
    const auto request = it->first;
    const auto deadline = it->second.m_deadline;
    if (Clock::now() < deadline) {
      m_changed.wait_until(lock, stop, deadline, [this, request] {
        return m_entries.empty() || m_entries.begin()->first != request;
      });
      continue;
    }
    auto route = std::move(it->second.m_route);
    m_entries.erase(it);
    lock.unlock();
    reportTimeout(route, request);
    lock.lock();
  }
}

}  // namespace pop
