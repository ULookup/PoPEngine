// 在独立的 mTLS 连接上交换小型统计快照，按内网查询号区分不同公网观察连接。
#include "engine/net/state_channel.h"

#include <utility>

#include "engine/net/pending_replies.h"
#include "engine/net/tls_transport.h"
#include "engine/net/wire_frame.h"
#include "services/game_worker.h"

namespace pop {
namespace {

// 用替换键向具体连接投递普通状态。跨网关共用的内网连接按查询号隔离，
// 单个观察者的公网连接则可合并同一 Space 的旧采样。
void SendState(const SendFrame& send, const wire::PublishedState& state,
               Id replace_key) {
  send(EncodeFrame(wire::PUBLISHED_STATE, state.SerializeAsString()),
       replace_key);
}

// 采样通道单独核算等待数，关闭时先停网络回调，再结束计时线程。
class SnapshotChannel final : public StateChannel {
 public:
  // 打开到已配置 Worker 的第二条长连接，仅接收此连接上的快照答复。
  SnapshotChannel(const GatewayNetwork& config, NetLimits limits)
      : m_pending(limits, OnTimeout),
        m_client(StartTlsClient(
            config.m_scene, config.m_internalTls, limits,
            [this](WireBytes bytes, SendFrame) { receive(bytes); })) {}

  // 入队前先关联发起连接，处理断线或满载时只向该观察者返回状态。
  void query(Id query_id, const FixtureRoute& route, SendFrame reply) override {
    const auto space = route.m_target.m_space;
    const auto ticket = m_pending.reserve(query_id, space, reply);
    if (!ticket) {
      ReplyStateStatus(reply, query_id, space, wire::STATE_BUSY);
      return;
    }

    wire::StateQuery query;
    query.set_query_id(ticket);
    query.set_space_id(space);
    query.set_space_owner_epoch(INITIAL_SPACE_OWNER_EPOCH);
    query.set_worker_instance_id(route.m_worker);
    auto admission = m_client->send(
        EncodeFrame(wire::STATE_QUERY, query.SerializeAsString()), ticket);
    if (admission != SendStatus::Queued) {
      wire::PublishedState refused;
      refused.set_query_id(ticket);
      refused.set_space_id(space);
      refused.set_status(wire::STATE_BUSY);
      deliver(std::move(refused));
    }
  }

 private:
  // 超时返回空的未知状态，迟到的快照已失去回送资格。
  static void OnTimeout(const ReplyRoute& route, Id) {
    ReplyStateStatus(route.m_send, route.m_clientId, route.m_space,
                     wire::STATE_UNKNOWN);
  }

  // 仅在快照类型、状态和关联范围匹配的情况下消费在途查询。
  void receive(const WireBytes& bytes) {
    auto frame = DecodeFrame(bytes);
    wire::PublishedState state;
    if (frame && frame->message_type() == wire::PUBLISHED_STATE &&
        state.ParseFromString(frame->payload()) && state.query_id() &&
        wire::StateStatus_IsValid(state.status()) &&
        (state.status() != wire::STATE_AVAILABLE || state.server_tick())) {
      deliver(std::move(state));
    }
  }

  // 从内网查询号恢复原查询号；同一采样最多向原连接回送一次。
  void deliver(wire::PublishedState state) {
    if (auto observer = m_pending.take(state.query_id(), state.space_id())) {
      state.set_query_id(observer->m_clientId);
      SendState(observer->m_send, state, observer->m_space);
    }
  }

  PendingReplies m_pending;             // 普通状态查询独享在途容量和截止调度。
  std::unique_ptr<TlsClient> m_client;  // 优先关闭网络及其回调。
};

}  // namespace

std::unique_ptr<StateChannel> StartStateChannel(const GatewayNetwork& config,
                                                NetLimits limits) {
  return std::make_unique<SnapshotChannel>(config, limits);
}

void ReplyStateStatus(const SendFrame& reply, Id query_id, SpaceId space,
                      wire::StateStatus status) {
  wire::PublishedState state;
  state.set_query_id(query_id);
  state.set_space_id(space);
  state.set_status(status);
  SendState(reply, state, space);
}

void ServeStateQuery(const wire::Envelope& envelope,
                     const std::string& instance,
                     const std::optional<FixtureBinding>& fixture,
                     const GameWorker& worker, const SendFrame& send) {
  wire::StateQuery query;
  const bool parsed = query.ParseFromString(envelope.payload());
  wire::PublishedState state;
  state.set_query_id(query.query_id());
  state.set_space_id(query.space_id());
  if (!parsed || !query.query_id() || !fixture ||
      query.space_id() != fixture->m_space) {
    state.set_status(wire::STATE_REJECTED);
  } else if (query.worker_instance_id() != instance ||
             query.space_owner_epoch() != INITIAL_SPACE_OWNER_EPOCH) {
    state.set_status(wire::STATE_STALE_ROUTE);
  } else if (auto snapshot = worker.snapshot(query.space_id())) {
    state.set_status(wire::STATE_AVAILABLE);
    state.set_server_tick(snapshot->m_tick);
    state.set_entities(snapshot->m_entities);
    state.set_inputs(snapshot->m_inputs);
    state.set_dispatched(snapshot->m_dispatched);
    state.set_result_commits(snapshot->m_resultCommits);
  } else {
    state.set_status(wire::STATE_STALE_ROUTE);
  }
  SendState(send, state, query.query_id());
}

}  // namespace pop
