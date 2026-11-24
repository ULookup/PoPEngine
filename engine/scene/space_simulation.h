// 声明可装配到一个 Space 的模拟逻辑，只在所属 Scene 分片上调用。
// 模块通过返回只读快照申请后台计算，计算线程无法访问模块实例。

#pragma once

#include <cstddef>
#include <functional>
#include <memory>
#include <optional>
#include <span>

#include "engine/runtime/mailbox.h"

namespace pop {

// 场景模块对一个实体的阶段快照发起的一次只读计算。
struct WorkRequest {
  Id m_stage;                 // 模块用于区分计算阶段的标识。
  Bytes m_snapshot;           // 独立的实体阶段快照，受后台通道字节预算约束。
  ComputeFunction m_compute;  // 仅依赖快照内容的计算函数。
};

// 一次实体步进对版本和后台队列的影响。
struct EntityStep {
  bool m_changed = false;             // 此步是否修改了模块持有的实体状态。
  std::optional<WorkRequest> m_work;  // 此步需要派发的任务，空值表示无工作。
};

// 每个 Space 独享的模拟模块，由 Scene 分片驱动其实体并提交结果。
class SpaceSimulation {
 public:
  // 随所属 Space 释放模块状态。
  virtual ~SpaceSimulation() = default;

  // 按一个固定步长推进绑定实体；input 仅在调用期间可借用，空值表示停止输入。
  virtual EntityStep advance(EntityBinding binding, Tick tick,
                             const ControlInput* input) = 0;

  // 在原任务的下一提交窗口接收已校验结果，output 仅在本次调用期间有效。
  virtual void commit(EntityBinding binding, Id stage,
                      std::span<const std::byte> output) = 0;
};

// 在所属分片上为指定 Space 构造独立模块，可返回空指针以空载运行。
using SimulationFactory =
    std::function<std::unique_ptr<SpaceSimulation>(SpaceId)>;

}  // namespace pop
