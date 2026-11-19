// 按进程角色加载配置、初始化日志并装配服务。
// 主线程等待退出信号，并负责有序停机和报告启动错误。

#include "apps/process.h"

#include <pthread.h>
#include <signal.h>

#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "engine/core/config.h"
#include "engine/core/log.h"
#include "services/game_worker.h"

namespace pop {
namespace {
// 一个进程角色及其负责启停的服务名，用于装配和生命周期日志。
struct Assembly {
  const char* m_role;                   // 配置与日志共用的角色名。
  std::vector<const char*> m_services;  // 按启动顺序排列的服务名。
};
// 将入口选定的角色映射到固定服务组。
Assembly Services(Role role) {
  switch (role) {
    case Role::Gateway:
      return {"gateway", {"GatewayService"}};
    case Role::GameWorker:
      return {"game_worker", {"PlayerService", "SceneService"}};
    case Role::Platform:
      return {"platform",
              {"AuthService", "SocialService", "ChatService", "MailService",
               "MarketService", "MatchService", "LeaderboardService"}};
    case Role::Control:
      return {"control", {"WorldCoordinator"}};
  }
  throw std::logic_error("unknown role");
}
// 取得唯一的 --config 路径；各角色使用同一启动约定。
std::string ConfigPath(int argc, char** argv) {
  if (argc != 3 || std::string_view(argv[1]) != "--config")
    throw std::runtime_error("usage: --config <path>");
  return argv[2];
}
}  // namespace

int RunProcess(Role role, int argc, char** argv) {
  auto assembly = Services(role);
  bool logging_ready = false;  // 初始化前的错误直接写 stderr。
  try {
    auto config = LoadConfig(ConfigPath(argc, argv), assembly.m_role);

    // 后续启动的线程继承信号屏蔽设置，由主线程统一等待停机信号。
    sigset_t signals;
    sigemptyset(&signals);
    sigaddset(&signals, SIGINT);
    sigaddset(&signals, SIGTERM);
    if (pthread_sigmask(SIG_BLOCK, &signals, nullptr) != 0)
      throw std::runtime_error("cannot block shutdown signals");
    StartLogging(assembly.m_role, config.m_logEntries);
    logging_ready = true;
    std::unique_ptr<GameWorker> worker;
    if (role == Role::GameWorker) worker = std::make_unique<GameWorker>(config);
    for (auto service : assembly.m_services)
      LOG_INFO("event=service_start service={}", service);
    LOG_INFO("event=ready");
    int signal = 0;
    if (sigwait(&signals, &signal) != 0)
      throw std::runtime_error("sigwait failed");
    if (worker) worker->stop();
    for (auto it = assembly.m_services.rbegin();
         it != assembly.m_services.rend(); ++it)
      LOG_INFO("event=service_stop service={}", *it);
    LOG_INFO("event=stopped signal={}", signal);
    StopLogging();
    return 0;
  } catch (const std::exception& error) {
    if (logging_ready) {
      LOG_ERROR("event=error reason={}", error.what());
    } else {
      std::cerr << "role=" << assembly.m_role
                << " event=error reason=" << error.what() << '\n';
    }
    StopLogging();
    return 1;
  }
}
}  // namespace pop
