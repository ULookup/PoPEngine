// 声明服务端进程的启动配置及其 YAML 加载入口。

#pragma once

#include <cstddef>
#include <string>
#include <vector>

#include "engine/core/types.h"

namespace pop {
// 校验后的本地进程配置；GameWorker 使用其中的线程和 Space 配置。
struct Config {
  std::size_t m_logEntries{};         // 异步日志队列能容纳的条数。
  unsigned m_playerShards{};          // 要启动的 Player 执行分片数。
  unsigned m_sceneShards{};           // 要启动的 Scene 执行分片数。
  unsigned m_computeThreads{};        // 处理只读任务的计算线程数。
  Budgets m_budgets{};                // 每个 Space 创建邮箱时采用的通道预算。
  std::vector<SpaceConfig> m_spaces;  // 本 Worker 承载的 Space 及其分片。
};

// 从 path 读取与 role 匹配的配置；缺项或绑定冲突时抛出异常。
Config LoadConfig(const std::string& path, const std::string& role);
}  // namespace pop
