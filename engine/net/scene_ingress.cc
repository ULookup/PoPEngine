// 在 mTLS 入口校验本实例的输入批次，回执由投递结果或 Scene 提交点产生。
#include <algorithm>
#include <cstring>
#include <future>
#include <stdexcept>
#include <utility>

#include "engine/net/realtime.h"
#include "engine/net/state_channel.h"
#include "engine/net/tls_transport.h"
#include "engine/net/wire_frame.h"
#include "services/game_worker.h"

namespace pop {
namespace {
// 将 Scene 终态转换成线上结果，队列覆盖与过期各自可见。
wire::InputStatus WireResult(InputResult result) {
  switch (result) {
    case InputResult::Applied:
      return wire::APPLIED;
    case InputResult::Expired:
      return wire::EXPIRED;
    case InputResult::Stale:
      return wire::STALE;
    case InputResult::Superseded:
      return wire::SUPERSEDED;
    case InputResult::Cancelled:
      return wire::REJECTED;
  }
  return wire::UNKNOWN;
}

// 翻译入队失败；成功入队的输入须等本 Tick 发布才回送。
wire::InputStatus Refusal(Admission admission) {
  switch (admission) {
    case Admission::UnknownSpace:
      return wire::STALE_ROUTE;
    case Admission::Full:
      return wire::BUSY;
    case Admission::Stale:
      return wire::STALE;
    case Admission::Expired:
      return wire::EXPIRED;
    default:
      return wire::REJECTED;
  }
}

// 将消息中的截止时间转换为本机单调时刻，然后通过正式入口投递。
void PostInput(GameWorker& worker, SpaceId space, const wire::RoutedInput& item,
               const SendFrame& send) {
  const auto request = item.request_id();
  const auto sequence = item.input_seq();
  const EntityBinding binding{
      .m_entity = item.entity_id(),
      .m_generation = item.entity_generation(),
      .m_sessionEpoch = item.session_epoch(),
  };
  const auto local_now = Clock::now();
  const auto wall_now = UnixMilliseconds();
  if (!request || !sequence || !binding.valid() || item.state().empty() ||
      item.state().size() > MAX_CONTROL_BYTES) {
    ReplyToInput(send, request, sequence, space, 0, wire::REJECTED);
    return;
  }
  if (item.expires_at_unix_ms() <= wall_now) {
    ReplyToInput(send, request, sequence, space, 0, wire::EXPIRED);
    return;
  }

  Bytes state(item.state().size());
  std::memcpy(state.data(), item.state().data(), state.size());
  // 受信网关和本机可能有小幅校时偏差，本地有效期始终以 250 ms 为上限。
  const Milliseconds remaining(
      std::min(item.expires_at_unix_ms() - wall_now, INPUT_LIFETIME.count()));
  ControlInput input{
      .m_binding = binding,
      .m_seq = sequence,
      .m_state = std::move(state),
      .m_validUntil = local_now + remaining,
  };
  const auto admission = worker.postInput(
      space, std::move(input), std::to_string(request),
      [send, request, sequence, space](InputResult result, Tick tick) {
        ReplyToInput(send, request, sequence, space, tick, WireResult(result));
      });
  if (admission != Admission::Accepted && admission != Admission::Merged) {
    ReplyToInput(send, request, sequence, space, 0, Refusal(admission));
  }
}
}  // namespace

std::unique_ptr<NetworkListener> StartSceneIngress(const SceneNetwork& config,
                                                   NetLimits limits,
                                                   const std::string& instance,
                                                   GameWorker& worker) {
  if (config.m_fixture) {
    BindEntity request{config.m_fixture->m_binding, {}};
    auto ready = request.m_reply.get_future();
    if (worker.bindEntity(config.m_fixture->m_space, std::move(request)) !=
            Admission::Accepted ||
        ready.wait_for(limits.m_timeout) != std::future_status::ready ||
        ready.get() != RequestStatus::Completed) {
      throw std::runtime_error("could not install isolated fixture binding");
    }
  }

  return StartTlsListener(
      config.m_endpoint, config.m_tls, limits,
      [&worker, instance, fixture = config.m_fixture](WireBytes bytes,
                                                      SendFrame send) {
        auto envelope = DecodeFrame(bytes);
        if (envelope && envelope->message_type() == wire::STATE_QUERY) {
          ServeStateQuery(*envelope, instance, fixture, worker, send);
          return;
        }
        if (envelope && envelope->message_type() != wire::INPUT_BATCH) {
          ReplyToInput(send, 0, 0, 0, 0, wire::UNSUPPORTED);
          return;
        }
        wire::InputBatch batch;
        if (!envelope || !batch.ParseFromString(envelope->payload()) ||
            !batch.space_id() || batch.inputs_size() == 0 ||
            batch.inputs_size() > MAX_INPUTS_PER_BATCH) {
          ReplyToInput(send, 0, 0, 0, 0, wire::REJECTED);
          return;
        }
        for (const auto& item : batch.inputs()) {
          if (batch.worker_instance_id() != instance ||
              batch.space_owner_epoch() != INITIAL_SPACE_OWNER_EPOCH) {
            ReplyToInput(send, item.request_id(), item.input_seq(),
                         batch.space_id(), 0, wire::STALE_ROUTE);
          } else {
            PostInput(worker, batch.space_id(), item, send);
          }
        }
      });
}

}  // namespace pop
