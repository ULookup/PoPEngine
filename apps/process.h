// 声明四类服务端进程共用的启动入口。调用方传入进程角色和命令行参数。

#pragma once

namespace pop {
// 选择本进程装配的服务组，也决定配置文件应声明的角色。
enum class Role {
  Gateway,     // 网关服务进程。
  GameWorker,  // 承载 Player 和 Scene 执行分片的进程。
  Platform,    // 承载认证和公共业务服务的进程。
  Control      // 承载世界协调服务的进程。
};

// 按 role 解析命令行并运行至停机；正常退出返回 0，启动或运行失败返回 1。
int RunProcess(Role role, int argc, char** argv);
}  // namespace pop
