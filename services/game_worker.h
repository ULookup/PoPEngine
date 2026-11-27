// 声明 GameWorker 的进程内接口，供启动入口装配分片、投递本地输入和停止运行。

#pragma once

#include <memory>
#include <optional>
#include <string>

#include "engine/core/config.h"
#include "engine/runtime/mailbox.h"
#include "engine/scene/space_simulation.h"

namespace pop {

// 进程入口持有的 GameWorker，管理分片的生命期并按 Space 路由进程内消息。
class GameWorker {
 public:
  // 按 config 创建通道、计算池和分片，在各分片上装配可选的模拟模块。
  explicit GameWorker(const Config& config,
                      SimulationFactory create_simulation = {});

  // 释放 Worker 的运行资源，回收尚在运行的线程。
  ~GameWorker();

  // 可信控制方向目标 Space 安装绑定。成功入队后从请求回执取得终态。
  Admission bindEntity(SpaceId space_id, BindEntity request);

  // 按目标 Space 投递完整输入，保留调用方的有效期与可选跟踪标识。
  Admission postInput(SpaceId space_id, ControlInput input,
                      std::string trace = {}, InputReply reply = {});

  // 复制目标 Space 最近一次发布的整步统计，未知 Space 返回空值。
  std::optional<Snapshot> snapshot(SpaceId space_id) const;

  // 停止处理新工作并回收线程，分片的运行异常在此报告给调用方。
  void stop();

 private:
  // 进程内的分片、任务和停机状态的私有实现。
  class Runtime;
  std::unique_ptr<Runtime> m_runtime;  // 由此 Worker 独占的运行实例。
};

}  // namespace pop
