// Control 进程入口，以世界协调服务的角色启动公共进程流程。

#include "apps/process.h"

// 将命令行交给世界协调进程的启动流程，并返回其退出状态。
int main(int argc, char** argv) {
  return pop::RunProcess(pop::Role::Control, argc, argv);
}
