// Platform 进程入口，选择承载认证与公共业务服务的进程角色。

#include "apps/process.h"

// 将命令行交给公共业务进程的启动流程，并返回其退出状态。
int main(int argc, char** argv) {
  return pop::RunProcess(pop::Role::Platform, argc, argv);
}
