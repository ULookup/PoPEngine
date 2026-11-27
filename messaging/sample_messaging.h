// 在调用方的基础设施线程上逐项投递 Outbox、消费持久事件和重放隔离消息。
#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>

#include "storage/sample_repository.h"

namespace pop {

// 一个分片的 AMQP 身份、路由和等待预算；URI 可携带凭据，禁止写入日志。
struct BrokerSettings {
  std::string m_uri;                              // 本次连接的 AMQP URI。
  std::string m_exchange = "pop.sample.events";   // 样例域持久交换机。
  std::string m_queue = "pop.sample.counter.0";   // 接收端订阅与分片。
  std::string m_routingKey = "sample.counter.0";  // 本分片计数事实的路由。
  std::chrono::milliseconds m_timeout{2000};      // 一次 MQ 往返预算。
  uint32_t m_maxAttempts = 8;                     // 隔离前的尝试次数。
};

// 一条 Outbox 认领的持久处理结果。
enum class RelayResult {
  Idle,         // 没有到期且可认领的事件。
  Published,    // Broker 已确认路由，且认领者已更新 Outbox。
  Deferred,     // 已安排沿用原事件号重试。
  Quarantined,  // 尝试预算耗尽，等待运维重放。
  Stale,        // 本次认领代次已失效。
};

// 一条消费投递的结果；成功返回前 Inbox 与样例业务变更已经提交。
enum class ConsumeResult {
  Empty,        // 当前订阅没有可取消息。
  Applied,      // 新事件事务已提交，投影已完成。
  Duplicate,    // Inbox 已记录本事件，投影已补齐。
  Deferred,     // 原消息已交还订阅，稍后重试。
  Quarantined,  // 原消息被送往隔离队列或已留在隔离队列。
};

// 单个基础设施线程串行持有并使用连接，每次最多一条待确认发布及一条投递。
// MQ 故障将关闭旧连接，后续调用重新连接。不得从 Scene Tick 直接调用。
class SampleMessaging {
 public:
  // 保存配置，连接推迟至第一次操作。
  explicit SampleMessaging(BrokerSettings settings);
  // 在所属线程上释放信道、连接和 libev 事件循环。
  ~SampleMessaging();

  // 声明持久 topic 交换机、quorum 订阅及其隔离队列，成功可重复调用。
  void declareTopology();
  // 短事务认领一条 Outbox，在事务外发送，按本次认领代次标记结果。
  RelayResult relayOne(SampleRepository& repository);
  // 发布一个已有事件；仅在收到确认且本次消息成功路由时返回 true。
  bool publish(const SampleEvent& event);
  // Inbox 与业务更新提交后调用可选投影，投影失败或崩溃会保留重投机会。
  ConsumeResult consumeOne(
      SampleRepository& repository,
      const std::function<void(const SampleEvent&)>& project = {});
  // 从隔离队列取一条按原身份重放；失败的消息留在该隔离队列。
  ConsumeResult replayOne(
      SampleRepository& repository,
      const std::function<void(const SampleEvent&)>& project = {});

 private:
  // 一条线程私有的事件循环、TCP 连接和 AMQP 信道。
  class Session;
  // 连接不可用时重新建立 AMQP 会话并启用发布确认。
  Session& session();
  // 根据来源队列选择消费失败后的处理；已收到的投递绑定到本次信道。
  ConsumeResult consume(SampleRepository& repository,
                        const std::function<void(const SampleEvent&)>& project,
                        bool replay);

  bool m_inProjection = false;  // 投影回调期间保留当前投递，禁止重入本实例。
  BrokerSettings m_settings;    // 连接和订阅约束。
  std::unique_ptr<Session> m_session;  // 当前调用线程独占。
};

}  // namespace pop
