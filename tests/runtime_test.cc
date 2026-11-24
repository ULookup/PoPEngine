// 使用可控时钟验证双 Space 独立推进、工作结果的版本校验及邮箱的预算和关闭行为。

#include <chrono>
#include <cmath>
#include <cstring>
#include <functional>
#include <future>
#include <iostream>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "engine/scene/scene_shard.h"
#include "services/game_worker.h"

namespace pop {
namespace {

using namespace std::chrono_literals;

// 断言可观察结果，失败时把用例提供的上下文交给测试入口输出。
void Check(bool condition, const std::string& message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

// 给四个通道设置相同的小预算，场景可单独收紧某个通道。
Budgets Limits() {
  Budgets budgets;
  budgets.fill({4, 4096, 1000ms, 2});
  return budgets;
}

// 构造验收模块认识的一字节控制状态，期限由用例的显式时钟决定。
Message Input(EntityBinding binding, Id seq, TimePoint now, Milliseconds ttl,
              std::string trace = {}, std::byte direction = std::byte{1}) {
  return {
      binding.m_entity,
      now,
      ControlInput{binding, seq, {direction}, now + ttl},
      std::move(trace),
  };
}

// 经控制队列安装实体，调用方推进下一步后可取得绑定回执。
std::future<RequestStatus> Bind(SpaceChannel& channel, EntityBinding binding,
                                TimePoint now) {
  BindEntity request{binding, {}};
  auto done = request.m_reply.get_future();
  const auto admission = channel.m_mailbox.post(
      {binding.m_entity, now, std::move(request), {}}, now);
  Check(admission == Admission::Accepted, "binding request rejected");
  return done;
}

// 走真实后台任务队列，把取件与回投时机留给用例控制。
Admission Dispatch(SpaceChannel& channel, Work work, TimePoint now) {
  return channel.m_mailbox.post({work.m_key.m_entity, now, std::move(work), {}},
                                now);
}

// 将一个坐标或距离复制成测试任务的进程内数据。
Bytes EncodeMeters(double value) {
  Bytes bytes(sizeof(value));
  std::memcpy(bytes.data(), &value, sizeof(value));
  return bytes;
}

// 读取测试任务的坐标或计算结果，验证快照长度。
double DecodeMeters(std::span<const std::byte> bytes) {
  double value{};
  Check(bytes.size() == sizeof(value), "wrong distance payload size");
  std::memcpy(&value, bytes.data(), sizeof(value));
  return value;
}

// 验收模块使用的只读计算，通过同一任务入口在真实计算池或用例中执行。
Bytes Distance(std::span<const std::byte> snapshot) {
  return EncodeMeters(std::abs(DecodeMeters(snapshot)));
}

// 单轴运动验收模块，记录所属 Space 的坐标和已提交的距离。
class Motion final : public SpaceSimulation {
 public:
  // 实体位置变化后计算到原点距离的阶段编号，仅对本模块有意义。
  static constexpr Id DISTANCE_STAGE = 7;

  std::map<Id, double> m_positions;  // 本场测试实体的有符号位置，单位为米。
  std::map<Id, double> m_distances;  // 本场各测试实体最近提交的距离，单位为米。

  // 真实线程用例以此回调通知等待方已经在 Scene 中提交。
  std::function<void(double)> m_onCommit;

  // 将完整控制状态解释为单轴方向，仅在实际移动时请求派生距离。
  EntityStep advance(EntityBinding binding, Tick,
                     const ControlInput* input) override {
    if (!input) {
      return {};
    }
    Check(input->m_state.size() == 1, "unexpected motion input size");
    auto direction = input->m_state[0];
    if (direction == std::byte{0}) {
      return {};
    }
    Check(direction == std::byte{1} || direction == std::byte{2},
          "invalid motion input");

    auto& position = m_positions[binding.m_entity];
    position += (direction == std::byte{1} ? 1 : -1) *
                std::chrono::duration<double>(TICK_STEP).count();
    return {true,
            WorkRequest{DISTANCE_STAGE, EncodeMeters(position), Distance}};
  }

  // 只接收调度器完成身份与版本校验的计算结果。
  void commit(EntityBinding binding, Id stage,
              std::span<const std::byte> output) override {
    Check(stage == DISTANCE_STAGE, "committed wrong stage");
    double distance = DecodeMeters(output);
    m_distances[binding.m_entity] = distance;
    if (m_onCommit) {
      m_onCommit(distance);
    }
  }
};

// 验证空场独立计时、同场实体序号隔离、固定步长、追帧上限和输入失效。
void TwoSpaces() {
  TimePoint start{};
  SpaceChannel a({37, 0}, Limits());
  SpaceChannel b({9, 0}, Limits());
  SceneShard scene(Dispatch);
  auto motion = std::make_unique<Motion>();
  auto& state = *motion;
  scene.addSpace(a, start, std::move(motion));

  scene.advance(start + 49ms);
  Check(a.snapshot().m_tick == 0, "A advanced before its deadline");
  scene.advance(start + 50ms);
  Check(a.snapshot().m_entities == 0 && a.snapshot().m_dispatched == 0,
        "empty A created an entity or a task");

  scene.addSpace(b, start + 50ms);
  EntityBinding left{10, 1, 1};
  EntityBinding right{11, 2, 1};
  auto now = start + 51ms;
  auto left_bound = Bind(a, left, now);
  auto right_bound = Bind(a, right, now);
  a.m_mailbox.post(Input(left, 7, now, 200ms), now);
  a.m_mailbox.post(Input(right, 7, now, 200ms, {}, std::byte{2}), now);
  b.m_mailbox.post(Input(left, 7, now, 200ms), now);

  scene.advance(start + 100ms);
  Check(left_bound.get() == RequestStatus::Completed &&
            right_bound.get() == RequestStatus::Completed,
        "bind did not complete");
  auto first = a.snapshot();
  Check(first.m_tick == 2 && first.m_entities == 2 && first.m_inputs == 2 &&
            state.m_positions.at(10) == 0.05 &&
            state.m_positions.at(11) == -0.05,
        "two entities with sequence 7 did not move independently");
  Check(b.snapshot().m_tick == 1 && b.snapshot().m_entities == 0 &&
            b.snapshot().m_inputs == 0,
        "input created a binding in empty B");

  now = start + 100ms;
  auto tasks = a.m_mailbox.take(Lane::Background, now, now);
  Check(tasks.size() == 2, "concurrent entity tasks lost at dispatch");

  // 模拟两个计算线程以派发的逆序完成任务。
  for (auto it = tasks.rbegin(); it != tasks.rend(); ++it) {
    auto result = Compute(std::get<Work>(it->m_message.m_payload));
    const auto admission = a.m_mailbox.post(
        {result.m_key.m_entity, now, std::move(result), {}}, now);
    Check(admission == Admission::Accepted, "entity result rejected");
  }

  Check(scene.advance(start + 250ms) == start + 250ms, "lost catch-up debt");
  Check(a.snapshot().m_tick == 4 && b.snapshot().m_tick == 3,
        "catch-up exceeded two ticks");
  Check(a.snapshot().m_resultCommits == 2 && state.m_distances.at(10) == 0.05 &&
            state.m_distances.at(11) == 0.05,
        "one entity displaced the other pending result");

  scene.advance(start + 250ms);
  Check(a.snapshot().m_tick == 5 && b.snapshot().m_tick == 4 &&
            state.m_positions.at(10) == 0.2 && state.m_positions.at(11) == -0.2,
        "catch-up lost a fixed step");

  scene.advance(start + 300ms);
  Check(state.m_positions.at(10) == 0.2 && state.m_positions.at(11) == -0.2 &&
            b.snapshot().m_dispatched == 0 && b.snapshot().m_resultCommits == 0,
        "expired input or empty Space produced work");

  now = start + 301ms;
  EntityBinding rebound{10, 1, 2};
  auto installed = Bind(a, rebound, now);
  auto stale = Bind(a, left, now);
  a.m_mailbox.post(Input(left, 8, now, 100ms), now);
  a.m_mailbox.post(Input(rebound, 1, now, 100ms), now);
  a.m_mailbox.post(Input(right, 7, now, 100ms), now);

  scene.advance(start + 350ms);
  Check(installed.get() == RequestStatus::Completed &&
            stale.get() == RequestStatus::StaleBinding &&
            a.snapshot().m_inputs == 3 && state.m_positions.at(10) == 0.25 &&
            state.m_positions.at(11) == -0.2,
        "rebind accepted old control, reset a peer or lost the new sequence");
}

// 验证匹配结果只提交一次，版本或阶段错误及提交窗口结束后的回投无效。
void WorkResults() {
  for (const std::string fault :
       {"none", "version", "stage", "late", "rebind"}) {
    TimePoint start{};
    SpaceChannel channel({17, 0}, Limits());
    SceneShard scene(Dispatch);
    auto motion = std::make_unique<Motion>();
    auto& state = *motion;
    scene.addSpace(channel, start, std::move(motion));
    EntityBinding binding{41, 3, 2};
    Bind(channel, binding, start);
    channel.m_mailbox.post(Input(binding, 1, start, 75ms, {}, std::byte{2}),
                           start);

    auto now = start + 50ms;
    scene.advance(now);
    auto tasks = channel.m_mailbox.take(Lane::Background, now, now);
    Check(tasks.size() == 1 && !tasks[0].m_expired, "expected one live task");

    auto result = Compute(std::get<Work>(tasks[0].m_message.m_payload));
    if (fault == "version") {
      --result.m_key.m_version;
    }
    if (fault == "stage") {
      ++result.m_key.m_stage;
    }
    if (fault == "late") {
      now += TICK_STEP;
      scene.advance(now);
    }
    if (fault == "rebind") {
      Bind(channel, {41, 3, 3}, now);
    }

    const auto admission = channel.m_mailbox.post({0, now, result, {}}, now);
    Check(admission == Admission::Accepted,
          "result rejected before reaching Scene");
    if (fault == "none") {
      channel.m_mailbox.post({0, now, result, {}}, now);
    }

    scene.advance(now + TICK_STEP);
    bool valid = fault == "none";
    Check(channel.snapshot().m_resultCommits == (valid ? 1u : 0u) &&
              channel.snapshot().m_dispatched == 1 &&
              state.m_positions.at(41) == -0.05,
          "incorrect result commit: " + fault);
    Check(valid ? state.m_distances.at(41) == 0.05 : state.m_distances.empty(),
          "unexpected published distance: " + fault);
  }
}

// 经真实 Worker 入口绑定实体、投递带期限的输入，等待后台结果回到所属分片。
void WorkerIngress() {
  Config config{};
  config.m_playerShards = 1;
  config.m_sceneShards = 1;
  config.m_computeThreads = 1;
  config.m_spaces = {{73, 0}};
  config.m_budgets = Limits();

  std::promise<double> committed;
  auto distance = committed.get_future();
  GameWorker worker(config, [&](SpaceId) {
    auto motion = std::make_unique<Motion>();
    motion->m_onCommit = [&committed, reported = false](double meters) mutable {
      if (!reported) {
        committed.set_value(meters);
      }
      reported = true;
    };
    return motion;
  });

  EntityBinding binding{13, 1, 1};
  BindEntity request{binding, {}};
  auto ready = request.m_reply.get_future();
  Check(worker.bindEntity(73, std::move(request)) == Admission::Accepted &&
            ready.wait_for(2s) == std::future_status::ready &&
            ready.get() == RequestStatus::Completed,
        "worker did not install binding");

  ControlInput input{binding, 1, {std::byte{2}}, Clock::now() + 2s};
  Check(worker.postInput(74, input) == Admission::UnknownSpace,
        "missing route accepted");
  Check(worker.postInput(73, input, "producer-13") == Admission::Accepted,
        "worker rejected routed input");
  Check(
      distance.wait_for(2s) == std::future_status::ready && distance.get() > 0,
      "worker did not commit a real compute result");

  worker.stop();
  Check(worker.postInput(73, input) == Admission::Closed,
        "stopped worker accepted input");
}

// 验证邮箱在合并、满载、过期及停机时的占用变化和请求回执。
void MailboxLimits() {
  auto budgets = Limits();
  TimePoint now{};
  auto binding = EntityBinding{1, 1, 1};
  auto small = Input(binding, 1, now, 100ms);
  auto replacement = Input(binding, 2, now, 100ms, std::string(64, 'a'));
  const auto max_bytes = replacement.bytes();
  budgets[static_cast<int>(Lane::Input)] = {1, max_bytes, 50ms, 1};
  budgets[static_cast<int>(Lane::Control)] = {3, 4096, 50ms, 1};
  SpaceChannel channel({101, 0}, budgets);
  auto& box = channel.m_mailbox;

  Check(box.post(std::move(small), now) == Admission::Accepted,
        "first input rejected");
  Check(box.post(std::move(replacement), now) == Admission::Merged,
        "newer full state did not replace queued input");
  Check(box.stats(Lane::Input, now).m_bytes == max_bytes,
        "wrong merged byte count");

  auto large_trace = Input(binding, 3, now, 100ms);
  large_trace.m_trace.reserve(max_bytes);
  Check(box.post(std::move(large_trace), now) == Admission::Full,
        "oversize replacement bypassed byte budget");
  Check(box.post(Input(binding, 1, now, 100ms), now) == Admission::Stale,
        "old sequence replaced new");
  Check(box.post(Input({2, 1, 1}, 3, now, 100ms), now) == Admission::Full,
        "different entity merged");

  auto oversized = Input(binding, 4, now, 100ms);
  std::get<ControlInput>(oversized.m_payload).m_state.reserve(max_bytes);
  Check(box.post(std::move(oversized), now) == Admission::Full,
        "input bytes bypassed replacement budget");

  auto stats = box.stats(Lane::Input, now + 25ms);
  Check(stats.m_entries == 1 && stats.m_merged == 1 && stats.m_rejected == 4 &&
            stats.m_oldestAge == 25ms,
        "wrong queue counters");
  auto messages = box.take(Lane::Input, now + 50ms, now + 50ms);
  Check(
      messages.size() == 1 && messages[0].m_expired &&
          std::get<ControlInput>(messages[0].m_message.m_payload).m_seq == 2 &&
          box.stats(Lane::Input, now + 50ms).m_bytes == 0,
      "age boundary or byte release failed");

  SceneShard scene(Dispatch);
  scene.addSpace(channel, now);

  // 按指定来源和时刻投递控制请求，核对入队结果后返回终态的 future。
  auto post_request = [&](Id source, Id id, TimePoint created,
                          Admission expected) {
    std::promise<RequestStatus> reply;
    auto completion = reply.get_future();
    const auto admission =
        box.post({source, created, Inspect{id, std::move(reply)}, {}}, created);
    Check(admission == expected,
          "unexpected control admission for request " + std::to_string(id));
    return completion;
  };

  auto expired = post_request(1, 1, now, Admission::Accepted);
  auto cancelled = post_request(1, 2, now + 10ms, Admission::Accepted);
  auto completed = post_request(2, 3, now + 10ms, Admission::Accepted);
  post_request(3, 4, now + 10ms, Admission::Full);
  scene.advance(now + 50ms);
  Check(expired.get() == RequestStatus::Expired &&
            completed.get() == RequestStatus::Completed,
        "independent sources did not get an expired and a completed receipt");
  Check(cancelled.wait_for(0ms) != std::future_status::ready,
        "per-source quota was bypassed");

  box.close();
  Check(box.post(Input(binding, 3, now, 100ms), now) == Admission::Closed,
        "closed mailbox accepted input");
  box.cancelRemaining();
  Check(cancelled.get() == RequestStatus::Cancelled,
        "accepted request lost during shutdown");
}

}  // namespace
}  // namespace pop

// 按 CTest 传入的名称运行一个场景，将断言失败转换为非零退出状态。
int main(int argc, char** argv) {
  try {
    if (argc != 2) {
      throw std::runtime_error("expected scenario name");
    }
    std::string scenario(argv[1]);
    if (scenario == "TwoSpaces") {
      pop::TwoSpaces();
    } else if (scenario == "WorkResults") {
      pop::WorkResults();
    } else if (scenario == "WorkerIngress") {
      pop::WorkerIngress();
    } else if (scenario == "MailboxLimits") {
      pop::MailboxLimits();
    } else {
      throw std::runtime_error("unknown scenario " + scenario);
    }
    return 0;
  } catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return 1;
  }
}
