// 用隔离样例订阅验证无法路由的发布、消费隔离及保留身份的运维重放。
#include <chrono>
#include <optional>
#include <stdexcept>
#include <thread>

#include "messaging/sample_messaging.h"

namespace pop {
namespace {

// 在 Broker 尚未完成异步死信转移时有界等待，返回第一次可观察的投递。
template <class Receive>
ConsumeResult WaitDelivery(Receive receive) {
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(8);
  do {
    auto result = receive();
    if (result != ConsumeResult::Empty) return result;
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  } while (std::chrono::steady_clock::now() < deadline);
  throw std::runtime_error("sample recovery subscription delivery timed out");
}

}  // namespace

// 为同一已提交事实制造无绑定路由和投影失败，再从隔离队列按原 ID 重放。
void CheckMessagingRecovery(SampleRepository& repository,
                            const BrokerSettings& settings) {
  auto recovery = settings;
  recovery.m_queue += ".recovery";
  recovery.m_routingKey += ".recovery";
  recovery.m_maxAttempts = 2;
  SampleMessaging broker(recovery);
  broker.declareTopology();

  const auto operation = repository.apply(
      {.m_operationId = 8401, .m_aggregateId = 84, .m_delta = 2});
  if (operation.m_status != SampleStatus::Committed) {
    throw std::runtime_error("sample recovery operation was not committed");
  }
  auto no_route = recovery;
  no_route.m_routingKey += ".unbound";
  SampleMessaging unrouted(no_route);
  const auto returned = unrouted.publish(operation.m_event);
  const auto delivered = broker.publish(operation.m_event);
  if (returned || !delivered) {
    throw std::runtime_error(
        "mandatory return or routed confirm misclassified");
  }

  // Inbox 已在回调前提交；故意让后续投影连续失败，保留原消息直至隔离。
  auto fail_projection = [](const SampleEvent&) {
    throw std::runtime_error("injected sample projection outage");
  };
  auto attempt = [&] { return broker.consumeOne(repository, fail_projection); };
  const auto deferred = WaitDelivery(attempt);
  const auto quarantined = WaitDelivery(attempt);
  if (deferred != ConsumeResult::Deferred ||
      quarantined != ConsumeResult::Quarantined) {
    throw std::runtime_error("failed projection did not reach quarantine");
  }

  std::optional<SampleEvent> restored;
  const auto replay = WaitDelivery([&] {
    return broker.replayOne(repository,
                            [&](const auto& event) { restored = event; });
  });
  const auto applications = repository.projectionApplications(84);
  if (replay != ConsumeResult::Duplicate || !restored ||
      restored->m_eventId != operation.m_event.m_eventId ||
      restored->m_operationId != operation.m_event.m_operationId ||
      restored->m_aggregateId != 84 || restored->m_version != 1 ||
      restored->m_value != 2 || applications != 1) {
    throw std::runtime_error(
        "quarantine replay lost identity or applied twice");
  }
}

}  // namespace pop
