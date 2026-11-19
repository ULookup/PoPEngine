// GameWorker 进程入口，选择承载 Player 和 Scene 分片的进程角色。

#include "apps/process.h"

// 将命令行交给 GameWorker 启动流程，并返回其退出状态。
int main(int argc, char** argv) {
  return pop::RunProcess(pop::Role::GameWorker, argc, argv);
}
