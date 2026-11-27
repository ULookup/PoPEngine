// 将公网样例输入映射到可信会话，在有界表中保存回送连接直到 Scene 回执或超时。
#include <openssl/crypto.h>

#include <array>
#include <fstream>
#include <stdexcept>
#include <utility>

#include "engine/core/log.h"
#include "engine/net/input_forwarder.h"
#include "engine/net/pending_replies.h"
#include "engine/net/quic_transport.h"
#include "engine/net/realtime.h"
#include "engine/net/state_channel.h"
#include "engine/net/tls_transport.h"
#include "engine/net/wire_frame.h"

namespace pop {
namespace {
// 隔离驱动每次验收生成一份独立的 32 字节随机凭据。
using FixtureToken = std::array<char, 32>;

// 只读取预期长度，避免误将其他文件作为验收密钥。
FixtureToken ReadToken(const std::string& path) {
  FixtureToken token;
  std::ifstream file(path, std::ios::binary);
  if (!file.read(token.data(), token.size()) ||
      file.peek() != std::char_traits<char>::eof()) {
    throw std::runtime_error("invalid isolated fixture token file");
  }
  return token;
}

// 网关持有适配器，逆序析构保证公网回调退出后再关闭内网和回执表。
class RealtimeGateway final : public NetworkListener {
 public:
  // TLS 与 QUIC 只负责传输，身份与转发校验走同一入口。
  RealtimeGateway(const GatewayNetwork& config, NetLimits limits)
      : m_route(config.m_route), m_pending(limits, OnTimeout) {
    if (m_route) {
      m_token = ReadToken(m_route->m_tokenFile);
    }
    m_scene = StartTlsClient(
        config.m_scene, config.m_internalTls, limits,
        [this](WireBytes bytes, SendFrame) { receiveReceipt(bytes); });
    m_forwarder = std::make_unique<InputForwarder>(
        *m_scene, limits,
        [this](Id request, Id seq, SpaceId space, wire::InputStatus status) {
          finish(request, seq, space, 0, status);
        });
    m_states = StartStateChannel(config, limits);
    auto receive = [this](WireBytes bytes, SendFrame reply) {
      forward(bytes, std::move(reply));
    };
    m_tcp =
        StartTlsListener(config.m_ingress, config.m_publicTls, limits, receive);
    m_quic = StartQuicListener(config.m_ingress, config.m_publicTls, limits,
                               receive);
  }

 private:
  // 请求已交给内网后，回执超时只能报告结果未知。
  static void OnTimeout(const ReplyRoute& route, Id request) {
    ReplyToInput(route.m_send, request, route.m_clientId, route.m_space, 0,
                 wire::UNKNOWN);
  }

  // 入队失败和权威回执均须匹配本次转发的序号及 Space。
  void finish(Id request, Id sequence, SpaceId space, Tick tick,
              wire::InputStatus status) {
    if (auto route = m_pending.take(request, space, sequence)) {
      ReplyToInput(route->m_send, request, sequence, space, tick, status);
    }
  }

  // 仅当显式安装了回环身份且等长凭据匹配时才允许样例协议。
  bool authorized(const std::string& token) const {
    return m_route && token.size() == m_token.size() &&
           CRYPTO_memcmp(token.data(), m_token.data(), m_token.size()) == 0;
  }

  // 对端内网连接已通过身份验证，回执仍需匹配本次转发的关联字段。
  void receiveReceipt(const WireBytes& bytes) {
    auto frame = DecodeFrame(bytes);
    wire::InputReceipt receipt;
    if (frame && frame->message_type() == wire::INPUT_RECEIPT &&
        receipt.ParseFromString(frame->payload()) &&
        wire::InputStatus_IsValid(receipt.status()) &&
        (receipt.status() != wire::APPLIED || receipt.server_tick() != 0)) {
      finish(receipt.request_id(), receipt.input_seq(), receipt.space_id(),
             receipt.server_tick(), receipt.status());
    }
  }

  // 客户端只能提供完整输入和序号，所有路由字段来自已装配的可信上下文。
  void forward(const WireBytes& bytes, SendFrame reply) {
    const auto expires = UnixMilliseconds() + INPUT_LIFETIME.count();
    auto frame = DecodeFrame(bytes);
    if (frame && frame->message_type() == wire::SAMPLE_STATE_QUERY) {
      wire::SampleStateQuery query;
      if (!query.ParseFromString(frame->payload()) || !query.query_id() ||
          !authorized(query.fixture_token())) {
        ReplyStateStatus(reply, query.query_id(), 0, wire::STATE_REJECTED);
      } else {
        m_states->query(query.query_id(), *m_route, std::move(reply));
      }
      return;
    }
    if (!frame || frame->message_type() != wire::SAMPLE_INPUT) {
      ReplyToInput(reply, 0, 0, 0, 0,
                   frame ? wire::UNSUPPORTED : wire::REJECTED);
      return;
    }
    wire::SampleInput sample;
    if (!sample.ParseFromString(frame->payload()) ||
        !authorized(sample.fixture_token()) || !sample.input_seq() ||
        sample.state().empty() || sample.state().size() > MAX_CONTROL_BYTES) {
      ReplyToInput(reply, 0, sample.input_seq(), 0, 0, wire::REJECTED);
      return;
    }

    const auto& target = m_route->m_target;
    const auto seq = sample.input_seq();
    const auto request = m_pending.reserve(seq, target.m_space, reply);
    if (!request) {
      ReplyToInput(reply, 0, seq, target.m_space, 0, wire::BUSY);
      return;
    }

    wire::RoutedInput input;
    input.set_request_id(request);
    input.set_input_seq(seq);
    input.set_entity_id(target.m_binding.m_entity);
    input.set_entity_generation(target.m_binding.m_generation);
    input.set_session_epoch(target.m_binding.m_sessionEpoch);
    input.set_expires_at_unix_ms(expires);
    input.mutable_state()->swap(*sample.mutable_state());

    LOG_INFO("event=gateway_input request={} space={} input_seq={}", request,
             target.m_space, seq);
    m_forwarder->post(target.m_space, INITIAL_SPACE_OWNER_EPOCH,
                      m_route->m_worker, std::move(input));
  }

  std::optional<FixtureRoute> m_route;  // 回环验收的目标；空时拒绝样例身份。
  FixtureToken m_token{};               // 启动时注入的验收凭据。
  PendingReplies m_pending;             // 有界的在途回执表。
  std::unique_ptr<TlsClient> m_scene;   // 已验证 Worker 身份的关键消息通道。
  std::unique_ptr<InputForwarder> m_forwarder;  // 批量投递输入，单独核算积压。
  std::unique_ptr<StateChannel> m_states;       // 普通状态使用独立连接与预算。
  std::unique_ptr<NetworkListener> m_tcp;       // TCP/TLS 兼容入口。
  std::unique_ptr<NetworkListener> m_quic;      // MsQuic 公网入口。
};

}  // namespace

std::unique_ptr<NetworkListener> StartRealtimeGateway(
    const GatewayNetwork& config, NetLimits limits) {
  return std::make_unique<RealtimeGateway>(config, limits);
}

}  // namespace pop
