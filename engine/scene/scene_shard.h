// 声明 Scene 分片的推进接口和每个 Space 的消息通道。
// 分片独占可变场景状态，其他线程通过通道投递消息或读取已提交快照。

#pragma once

#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>

#include "engine/runtime/mailbox.h"
#include "engine/scene/space_simulation.h"

namespace pop {
// 一个 Space 的跨线程访问入口。GameWorker 持有它直至生产者退出。
class SpaceChannel {
 public:
  // 绑定 Space 的配置，并按 budgets 创建其专属邮箱。
  SpaceChannel(SpaceConfig config, Budgets budgets);
  const SpaceConfig m_config;  // 固定的 Space 身份及其所属分片。
  Mailbox m_mailbox;           // 该 Space 的生产者与消费者交换消息的队列。
  // 在锁内复制最近发布的状态，调用方可在锁外读取该副本。
  Snapshot snapshot() const;

 private:
  friend class SceneShard;
  // 由所属 Scene 分片在提交点替换已发布快照。
  void publish(const Snapshot& snapshot);
  mutable std::mutex m_mutex;  // 保护快照的复制与替换。
  Snapshot m_snapshot;         // 供其他线程读取的最近一次已提交状态。
};

// 在一条 Scene 执行线程上轮流推进多个 Space，各场独立计时和提交。
class SceneShard {
 public:
  // 向指定 Space 派发快照任务，返回计算队列的投递结果。
  using Dispatch = std::function<Admission(SpaceChannel&, Work, TimePoint)>;
  // 保存计算任务的派发入口，初始时尚未承载 Space。
  explicit SceneShard(Dispatch dispatch);
  // 接管 channel 对应的 Space；首步在 now 加一个固定步长后到期。
  void addSpace(SpaceChannel& channel, TimePoint now,
                std::unique_ptr<SpaceSimulation> simulation = {});
  // 在 now 推进各场已到期的步，返回所有 Space 中最早的下一截止点。
  TimePoint advance(TimePoint now);

 private:
  // 一个实体的权威控制绑定、输入水位及当前结果提交窗口。
  struct Entity {
    EntityBinding m_binding;                // 已在 Scene 上安装的控制绑定。
    Id m_version = 1;                       // 输入或模拟状态改变时递增。
    Id m_inputSeq = 0;                      // 本次绑定最后处理的输入序号。
    std::optional<ControlInput> m_input{};  // 当前持续生效的完整控制状态。
    std::optional<WorkKey> m_pending{};     // 本实体上一 Tick 成功派发的任务。
  };
  // 分片独占的一场模拟，场内实体共享时钟，各自拥有输入和任务状态。
  struct Space {
    SpaceChannel& m_channel;                        // 消息入口和快照发布位置。
    TimePoint m_nextTick;                           // 下一步的单调时钟截止点。
    std::unique_ptr<SpaceSimulation> m_simulation;  // 可选的场景模块。
    std::map<Id, Entity> m_entities{};  // 按实体标识排序的权威状态。
    Snapshot m_state{};                 // 本场已提交的运行统计。
    Id m_droppedResults = 0;            // 本场丢弃的回投总数。
  };
  // 安装到期的控制绑定，接收结果和输入，推进模块并发布此步统计。
  void step(Space& space, TimePoint now);
  // 在当前结果窗口按派发顺序提交匹配项，一项最多提交一次。
  void receiveResults(Space& space, TimePoint now);
  // 采纳本步截止前的输入，按实体及会话维护独立的序号。
  void receiveInputs(Space& space, TimePoint now);
  // 安装由可信控制方提交的绑定，新实体或更高代次开始新的输入序列。
  RequestStatus bindEntity(Space& space, EntityBinding binding);
  // 推进一场的实体状态，只派发场景模块在本步实际请求的任务。
  void advanceEntities(Space& space, TimePoint now);
  Dispatch m_dispatch;                // 分片向计算队列投递工作的回调。
  std::map<SpaceId, Space> m_spaces;  // 按 Space 标识组织的本分片权威状态。
};
}  // namespace pop
