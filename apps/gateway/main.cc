// Gateway 进程入口，以网关角色启动公共进程流程。

#include "apps/process.h"

// 将命令行交给网关启动流程，并向操作系统返回其退出状态。
int main(int argc, char** argv) {
  return pop::RunProcess(pop::Role::Gateway, argc, argv);
}
