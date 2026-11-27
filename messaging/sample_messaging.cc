// 在基础设施线程驱动单个 AMQP 信道；数据库事务和缓存访问均在回调结束后执行。
#include "messaging/sample_messaging.h"

#include <amqpcpp.h>
#include <amqpcpp/libev.h>
#include <ev.h>

#include <algorithm>
#include <chrono>
#include <limits>
#include <memory>
#include <optional>
#include <random>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>

#include "sample_event.pb.h"

namespace pop {
namespace {

using namespace std::chrono_literals;

constexpr char EVENT_TYPE[] = "counter.changed";
constexpr char SOURCE_DOMAIN[] = "sample";
constexpr uint32_t SCHEMA_VERSION = 1;
constexpr size_t MAX_EVENT_BYTES = 1024;

// 创建线程私有的事件循环，失败时阻止建立 AMQP 会话。
struct ev_loop* CreateLoop() {
  auto* loop = ev_loop_new(EVFLAG_AUTO);
  if (!loop) throw std::runtime_error("AMQP event loop allocation failed");
  return loop;
}

// 连接错误标记由同一事件循环读写，向同步调用方报告失败。
class BrokerHandler final : public AMQP::LibEvHandler {
 public:
  // 将 TCP 和心跳事件注册到当前线程的 libev 循环。
  explicit BrokerHandler(struct ev_loop* loop) : LibEvHandler(loop) {}
  // 连接、认证或协议握手失败后不再复用信道。
  void onError(AMQP::TcpConnection*, const char*) override { m_broken = true; }
  // TCP 丢失时未确认的发布和投递均由持久路径重试。
  void onLost(AMQP::TcpConnection*) override { m_broken = true; }

  bool m_broken = false;  // 当前会话已失败或超过等待预算。
};

// 一次 basic.get 取得的有界载荷、服务端投递序号与此前失败次数。
struct Delivery {
  std::string m_body;       // 已收齐的有限长 Protobuf 字节。
  uint64_t m_tag = 0;       // 当前信道内的投递标识，零表示空队列。
  uint64_t m_failures = 0;  // quorum 队列提供的失败次数。
  bool m_oversize = false;  // 超过预算的消息仅排空并转入隔离。
};

// 将相同 Outbox 行在不同认领代次下编码为同一个业务事件。
std::string EncodeEvent(const SampleEvent& event) {
  sample::CounterEvent wire;
  wire.set_event_type(EVENT_TYPE);
  wire.set_source_domain(SOURCE_DOMAIN);
  wire.set_payload_schema_id(SCHEMA_VERSION);
  wire.set_event_id(event.m_eventId);
  wire.set_operation_id(event.m_operationId);
  wire.set_aggregate_id(event.m_aggregateId);
  wire.set_aggregate_version(event.m_version);
  wire.set_value(event.m_value);
  wire.set_committed_at_ms(event.m_committedAtMs);
  return wire.SerializeAsString();
}

// 仅接受本样例域已支持的信封和绝对计数载荷。
std::optional<SampleEvent> DecodeEvent(const Delivery& delivery) {
  sample::CounterEvent wire;
  if (delivery.m_oversize || !wire.ParseFromString(delivery.m_body) ||
      wire.event_type() != EVENT_TYPE ||
      wire.source_domain() != SOURCE_DOMAIN ||
      wire.payload_schema_id() != SCHEMA_VERSION || !wire.event_id() ||
      wire.event_id() != wire.operation_id() || !wire.aggregate_id() ||
      !wire.aggregate_version() || !wire.committed_at_ms() || !wire.value()) {
    return std::nullopt;
  }
  return SampleEvent{.m_eventId = wire.event_id(),
                     .m_operationId = wire.operation_id(),
                     .m_aggregateId = wire.aggregate_id(),
                     .m_version = wire.aggregate_version(),
                     .m_value = wire.value(),
                     .m_committedAtMs = wire.committed_at_ms()};
}

// 限制每次退避的等待时间，并用抖动分散多个 Relay 的恢复流量。
std::chrono::milliseconds RetryDelay(uint64_t attempt) {
  const int ceiling = 25 << std::min<uint64_t>(attempt, 5);
  thread_local std::mt19937 random(std::random_device{}());
  return std::chrono::milliseconds(
      std::uniform_int_distribution<int>(ceiling / 2, ceiling)(random));
}

}  // namespace

// 一个专用信道最多保留一个未确认发布，basic.return 与随后的 ACK
// 属于同一次发布。
class SampleMessaging::Session {
 public:
  // 启用确认模式，从连接、握手到信道就绪受同一等待预算限制。
  explicit Session(const BrokerSettings& settings)
      : m_timeout(settings.m_timeout),
        m_loop(CreateLoop(), ev_loop_destroy),
        m_handler(m_loop.get()),
        m_connection(&m_handler, AMQP::Address(settings.m_uri)),
        m_channel(&m_connection) {
    m_channel.onError([this](const char*) { m_handler.m_broken = true; });
    m_channel.recall().onBegin(
        [this](int16_t, const std::string&) { m_returned = true; });
    m_channel.confirmSelect()
        .onAck([this](uint64_t tag, bool multiple) {
          if (tag == m_publishTag || (multiple && !tag)) {
            m_confirmed = true;
            m_done = true;
          }
        })
        .onNack([this](uint64_t tag, bool multiple, bool) {
          if (tag == m_publishTag || (multiple && !tag)) m_done = true;
        })
        .onSuccess([this] { m_done = true; });
    wait();
  }

  // 到期、失联或协议错误的会话在下次调用前重新创建。
  bool broken() const { return m_handler.m_broken; }

  // 提前创建隔离队列，以便拒绝消费的消息也可由 quorum 确认后移交。
  void declareTopology(const BrokerSettings& settings) {
    const auto quarantine = settings.m_queue + ".quarantine";
    AMQP::Table arguments;
    arguments["x-queue-type"] = "quorum";
    arguments["x-overflow"] = "reject-publish";
    arguments["x-max-length-bytes"] = int32_t{16 * 1024 * 1024};
    arguments["x-delivery-limit"] = int32_t{-1};
    m_channel.declareExchange(settings.m_exchange, AMQP::topic, AMQP::durable);
    m_channel.declareQueue(quarantine, AMQP::durable, arguments);

    arguments["x-delivery-limit"] =
        static_cast<int32_t>(settings.m_maxAttempts);
    arguments["x-dead-letter-exchange"] = "";
    arguments["x-dead-letter-routing-key"] = quarantine;
    arguments["x-dead-letter-strategy"] = "at-least-once";
    m_channel.declareQueue(settings.m_queue, AMQP::durable, arguments);
    m_channel
        .bindQueue(settings.m_exchange, settings.m_queue, settings.m_routingKey)
        .onSuccess([this] { m_done = true; });
    wait();
  }

  // 等待同次 publish 的 confirm，持久行的更新由上层在返回后执行。
  bool publish(const BrokerSettings& settings, const SampleEvent& event) {
    const auto body = EncodeEvent(event);
    AMQP::Envelope envelope(body.data(), body.size());
    envelope.setPersistent();
    envelope.setMessageID(std::to_string(event.m_eventId));
    envelope.setTypeName(EVENT_TYPE);
    envelope.setContentType("application/x-protobuf");
    m_returned = false;
    m_confirmed = false;
    ++m_publishTag;
    if (!m_channel.publish(settings.m_exchange, settings.m_routingKey, envelope,
                           AMQP::mandatory)) {
      m_handler.m_broken = true;
    }
    wait();
    return m_confirmed && !m_returned;
  }

  // 按片段收集不超过样例预算的载荷，拿到投递序号才返回给事务调用方。
  Delivery get(const std::string& queue) {
    m_delivery = {};
    m_channel.get(queue)
        .onSize([this](uint64_t bytes) {
          m_delivery.m_oversize = bytes > MAX_EVENT_BYTES;
        })
        .onHeaders([this](const AMQP::MetaData& meta) {
          if (meta.hasHeaders() &&
              meta.headers().contains("x-delivery-count")) {
            m_delivery.m_failures = meta.headers().get("x-delivery-count");
          }
        })
        .onData([this](const char* data, size_t bytes) {
          if (!m_delivery.m_oversize) m_delivery.m_body.append(data, bytes);
        })
        .onComplete([this](uint64_t tag, bool) {
          m_delivery.m_tag = tag;
          m_done = true;
        })
        .onEmpty([this] { m_done = true; });
    wait();
    return std::move(m_delivery);
  }

  // ACK 或拒绝该投递；同步信道命令让调用方返回前 Broker 已处理此前各帧。
  void settle(uint64_t tag, bool acknowledged, bool requeue) {
    const bool queued =
        acknowledged ? m_channel.ack(tag)
                     : m_channel.reject(tag, requeue ? AMQP::requeue : 0);
    if (!queued) m_handler.m_broken = true;
    m_channel.setQos(1).onSuccess([this] { m_done = true; });
    wait();
  }

 private:
  // 按有界截止时间驱动 libev；超期时弃用整个信道，迟到确认不可关联下次发布。
  void wait() {
    ev_timer deadline;
    ev_timer_init(
        &deadline,
        [](auto*, ev_timer* timer, int) {
          static_cast<BrokerHandler*>(timer->data)->m_broken = true;
        },
        std::chrono::duration<double>(m_timeout).count(), 0);
    deadline.data = &m_handler;
    ev_now_update(m_loop.get());
    ev_timer_start(m_loop.get(), &deadline);
    while (!m_done && !broken()) ev_run(m_loop.get(), EVRUN_ONCE);
    ev_timer_stop(m_loop.get(), &deadline);
    m_done = false;
    if (broken()) throw std::runtime_error("AMQP unavailable or timed out");
  }

  std::chrono::milliseconds m_timeout;  // 一次操作的等待预算。
  bool m_done = false;                  // 当前 AMQP 命令已收到响应。
  bool m_confirmed = false;             // 当前发布已获 ACK。
  bool m_returned = false;              // 当前发布已经 basic.return 退回。
  uint64_t m_publishTag = 0;            // 确认模式下本信道已发布的数量。
  Delivery m_delivery;                  // 当前 get 回调使用的有界接收缓冲。

  // 依赖者后构造先销毁，连接析构期间 handler 和事件循环仍然存活。
  std::unique_ptr<struct ev_loop, decltype(&ev_loop_destroy)> m_loop;
  BrokerHandler m_handler;           // 此连接的事件源。
  AMQP::TcpConnection m_connection;  // 当前线程驱动的 TCP 连接。
  AMQP::TcpChannel m_channel;        // 唯一投递及发布信道。
};

SampleMessaging::SampleMessaging(BrokerSettings settings)
    : m_settings(std::move(settings)) {
  if (m_settings.m_uri.empty() || m_settings.m_exchange.empty() ||
      m_settings.m_exchange.size() > 255 || m_settings.m_queue.empty() ||
      m_settings.m_queue.size() > 244 || m_settings.m_routingKey.empty() ||
      m_settings.m_routingKey.size() > 255 || m_settings.m_timeout <= 0ms ||
      m_settings.m_timeout > 60s || !m_settings.m_maxAttempts ||
      m_settings.m_maxAttempts > std::numeric_limits<int32_t>::max()) {
    throw std::invalid_argument("invalid AMQP sample settings");
  }
}

SampleMessaging::~SampleMessaging() = default;

SampleMessaging::Session& SampleMessaging::session() {
  if (m_inProjection) throw std::logic_error("AMQP projection reentered");
  if (!m_session || m_session->broken()) {
    m_session = std::make_unique<Session>(m_settings);
  }
  return *m_session;
}

void SampleMessaging::declareTopology() {
  session().declareTopology(m_settings);
}

bool SampleMessaging::publish(const SampleEvent& event) {
  try {
    return session().publish(m_settings, event);
  } catch (const std::runtime_error&) {
    return false;
  }
}

RelayResult SampleMessaging::relayOne(SampleRepository& repository) {
  if (m_inProjection) throw std::logic_error("AMQP projection reentered");
  const auto claim = repository.claimOutbox(3 * m_settings.m_timeout + 1s);
  if (!claim) return RelayResult::Idle;

  if (publish(claim->m_event)) {
    return repository.markPublished(*claim) ? RelayResult::Published
                                            : RelayResult::Stale;
  }

  const bool quarantine = claim->m_attempt >= m_settings.m_maxAttempts;
  if (!repository.deferOutbox(*claim, RetryDelay(claim->m_attempt),
                              quarantine)) {
    return RelayResult::Stale;
  }
  return quarantine ? RelayResult::Quarantined : RelayResult::Deferred;
}

ConsumeResult SampleMessaging::consumeOne(
    SampleRepository& repository,
    const std::function<void(const SampleEvent&)>& project) {
  return consume(repository, project, false);
}

ConsumeResult SampleMessaging::replayOne(
    SampleRepository& repository,
    const std::function<void(const SampleEvent&)>& project) {
  return consume(repository, project, true);
}

ConsumeResult SampleMessaging::consume(
    SampleRepository& repository,
    const std::function<void(const SampleEvent&)>& project, bool replay) {
  auto& broker = session();
  const auto delivery = broker.get(replay ? m_settings.m_queue + ".quarantine"
                                          : m_settings.m_queue);
  if (!delivery.m_tag) return ConsumeResult::Empty;

  auto event = DecodeEvent(delivery);
  if (!event) {
    broker.settle(delivery.m_tag, false, replay);
    return ConsumeResult::Quarantined;
  }

  bool applied = false;
  try {
    applied = repository.applyInbox(*event);
    if (project) {
      m_inProjection = true;
      project(*event);
      m_inProjection = false;
    }
  } catch (...) {
    m_inProjection = false;
    const bool terminal =
        replay || delivery.m_failures >= m_settings.m_maxAttempts - 1;
    if (!terminal) std::this_thread::sleep_for(RetryDelay(delivery.m_failures));
    broker.settle(delivery.m_tag, false, replay || !terminal);
    return terminal ? ConsumeResult::Quarantined : ConsumeResult::Deferred;
  }

  broker.settle(delivery.m_tag, true, false);
  return applied ? ConsumeResult::Applied : ConsumeResult::Duplicate;
}

}  // namespace pop
