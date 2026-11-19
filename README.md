# PopEngine

本指南说明如何在 Linux 上构建、启动和验收第一阶段的四个服务端进程。默认配置让 GameWorker 在同一分片推进两个空 Space，Gateway、Platform 和 Control 提供各自的进程入口与模块生命周期。阶段范围见[开发计划](DevelopPlan.md)和[第一阶段方案](docs/Phase1.md)。

## 构建

在仓库根目录执行。已验证的开发环境为 x86_64 Linux、G++ 11.4、CMake/CTest 3.22、Ninja 和 vcpkg。进程验收用例使用 Python 3 标准库。将 `VCPKG_ROOT` 指向已引导的 vcpkg 目录，工程会按清单安装 yaml-cpp、spdlog 和 Lua。

```bash
export VCPKG_ROOT="$HOME/tools/vcpkg"
cmake --preset linux-gcc
cmake --build --preset linux-gcc
ctest --preset linux-gcc
```

四个可执行文件位于 `build/linux-gcc/`。要复现首次配置，使用一份源码副本，从空构建目录执行同一组命令。vcpkg 的共享二进制缓存可复用。

## 本地运行

在不同终端中分别执行所需的进程。它们可按任意顺序独立启动，看到 `event=ready` 即完成本地装配。按 Ctrl+C 或向 PID 发送 SIGTERM，等候 `event=stopped` 和退出码 0。

```bash
build/linux-gcc/gateway --config configs/gateway.yaml
build/linux-gcc/game_worker --config configs/game_worker.yaml
build/linux-gcc/platform --config configs/platform.yaml
build/linux-gcc/control --config configs/control.yaml
```

GameWorker 接受空的放置列表 `spaces: []`，可在尚未分配 Space 时启动。每个已放置的 Space 只需指定 `space_id` 和 `scene_shard`。空场每 50 ms 推进一步，首步及每 20 步输出一次 `event=tick`。其中 `entities` 是绑定数，`inputs`、`dispatched` 和 `commits` 分别累计已处理的输入、已派发的任务和已提交的结果。空场这四项均为 0。`tick_lag_ms` 表示相对本步截止点的迟到量。[GameWorker 配置](configs/game_worker.yaml)

进程内生产者通过 `GameWorker::bindEntity` 在目标 Space 安装实体及会话绑定，收到完成回执后，通过 `postInput(space_id, input, trace)` 投递完整控制状态。旧配置中的实体字段会被忽略。原 `game_worker --input` 移动演示已迁至显式验收场景，以下命令走实际 GameWorker、计算线程和 Scene 提交路径。

```bash
ctest --preset linux-gcc -R '^WorkerIngress$' -V
```

每个 Space 的四类队列分别限制条数、字节和年龄，`per_source` 限制一次取件中同一来源的数量。日志中的 `lane` 值 0、1、2、3 依次对应控制请求、连续输入、工作结果、后台任务。队列字节预算计入消息对象大小、跟踪串容量以及输入、快照或结果缓冲区的容量。结构化日志使用有界异步缓冲，写端过载时可覆盖最旧的日志条目。

## C++ 日志

在进程、分片和服务代码中包含项目日志头文件。两个宏都支持编译期格式检查，可直接传入格式串和参数。

```cpp
#include "engine/core/log.h"

LOG_INFO("event=space_started space={} tick={}", space_id, tick);
LOG_ERROR("event=space_failed space={} reason={}", space_id, reason);
```

进程入口在配置校验后初始化日志。普通事件写入 stdout，错误写入 stderr，均携带进程角色和实例标识。配置解析等早期错误直接写入 stderr。

## 复现验收

| CTest 用例 | 可以核对的行为 |
| --- | --- |
| `TwoSpaces` | 错峰启动两个 Space，在 A 绑定两个实体，用相同序号向相反方向移动。检查 B 空载推进、追帧上限、输入失效和会话重绑。 |
| `WorkResults` | 检查正确结果只提交一次，版本或阶段错误、迟到以及绑定失效后的结果被丢弃。 |
| `MailboxLimits` | 验证输入合并、字节预算、出队年龄、来源配额，以及控制请求的完成、过期和停机取消回执。 |
| `WorkerIngress` | 经进程内入口安装绑定并投递输入，等待真实计算线程的结果在 Scene 提交，同时检查未知 Space 和停机后的投递结果。 |
| `ProcessLifecycle` | 启动四个实际进程，检查空场持续推进且后台工作量为零，发送 SIGINT 或 SIGTERM 并等待各进程退出。 |
| `UnassignedWorker` | 以空放置列表启动 Worker，验证分片就绪、无场景创建和有序停机。 |
| `InvalidConfig` | 用错误角色、缺失队列预算和重复 Space 绑定启动，检查退出状态与报错字段。 |

单独复现旧结果时可执行 `ctest --preset linux-gcc -R '^WorkResults$' -V`。整轮执行输出保存在 `build/linux-gcc/Testing/Temporary/LastTest.log`。
