# 第三阶段 · 数据访问、缓存与可靠消息

本方案说明一个样例计数器如何在回包丢失、消息中断和缓存丢失后恢复，以及如何在 Linux 上复现实验。服务调用方在提交前保存操作号。样例一笔操作只产生一个事件，因此可复用操作号作为事件号。聚合版本从 1 开始，每次成功增加计数时递增。业务约束见 [TDD 第 8 节](TDD.md#8-持久化与数据访问)和[第 11 节](TDD.md#11-可靠事件与消息队列)。

## 实现与完成点

| 交付项 | 调用方与数据归属 | 完成点与恢复方式 |
| --- | --- | --- |
| 接受命令 | 逻辑线程用 `DatabaseExecutor::postApply` 或 `postQuery` 非阻塞投递。每个 `SampleRepository` 绑定一个分片的 Router 主写入口及连接池。执行池为自己的待处理消息设置条数与字节上限。 | 入队失败由调用方处理。受理后，数据库线程执行整笔事务并释放连接，再将值交给完成回调。调用方可在回调中投递所属分片。 |
| MySQL 权威提交 | InnoDB 在 READ COMMITTED 下按操作号串行化重试，再锁定该聚合行。ODB 映射操作结果，条件更新与批次查询通过当次 ODB 事务的原生连接执行参数化 SQL。 | 计数、版本、操作结果、Outbox 在同一事务提交。相同操作号及参数返回原结果，参数冲突或提交前到期返回拒绝。已开始提交却失去确认时返回 `Unknown`，调用方沿原号查询。 |
| 可靠发布 | 基础设施线程调用 `SampleMessaging::relayOne`。短事务认领一条到期 Outbox 并记录认领代次，退出事务后向持久 topic exchange 发布。 | 收到 publisher confirm ACK 且没有 mandatory return 后才标记已投递。旧认领的回包不能结束新认领。失败退避，达到预算的行保留为隔离状态，可用 `replayOutbox` 按原事件号重开。 |
| 消费与补投 | `consumeOne` 从指定 quorum queue 接收。消费事务同时记录 Inbox 和样例计数投影；保存每个事件身份及其参数。 | 同一事实重投只产生一次持久修改。缓存投影成功后才 ACK；消费事务已提交而缓存写失败时，重投可补写缓存。达到尝试预算转入该订阅的隔离队列，`replayOne` 保留原事件身份。 |
| 缓存重建 | `SampleReadView` 通过 redis-plus-plus 访问 Redis Cluster。每个聚合的版本与绝对计数写入同一个哈希键。 | `beginRebuild` 返回该聚合的持久快照版本，`finishRebuild` 读取本次高水位并分页追赶。Redis 原子比较版本，旧事件不能覆盖新值。更晚的提交由订阅继续投影。 |

五张 `sample_*` 表只在显式加载样例 DDL 的库中创建。四个现有进程沿用原启动配置。阶段三的无界面驱动装配数据模块和真实 GameWorker，登录、资产和战斗协议由后续阶段的使用方接入。[样例 DDL](../storage/schema.sql) · [数据库入口](../storage/sample_repository.h) · [消息入口](../messaging/sample_messaging.h) · [缓存入口](../engine/cache/sample_read_view.h)

## 构建与隔离验收

在项目原有构建依赖之外，启用 `POP_BUILD_DATA` 时须能找到下列开发包。可用 `CMAKE_PREFIX_PATH` 指定本机 SDK 前缀。

| 组件 | 构建所需内容 |
| --- | --- |
| MySQL / ODB | MySQL 8 客户端头文件和库、相互匹配的 `odb` 编译器、`libodb` 与 `libodb-mysql`。CMake 在构建目录生成 C++ 映射。本机使用 ODB 2.5.0 发行包，许可声明随包保存在 `/usr/share/doc/libodb/copyright` 和 `/usr/share/doc/libodb-mysql/copyright`，GPLv2 全文在 `/usr/share/common-licenses/GPL-2`。[上游许可文本与适用组件](https://www.codesynthesis.com/products/odb/license.xhtml) |
| AMQP | 启用了 Linux TCP 支持并提供 CMake 配置的 AMQP-CPP、libev 开发包。事件信封沿用项目的 Protobuf 工具链生成。 |
| Redis | 相互匹配的 redis-plus-plus 与 hiredis 头文件和库。 |

在可由当前用户启动 Docker 容器的 Linux 主机上运行。测试需要能够访问本次 Docker 私网桥接地址，本机安装 MySQL Router，或以 `POP_MYSQLROUTER` 指定其可执行文件路径。

```bash
export VCPKG_ROOT="$HOME/tools/vcpkg"
cmake --preset linux-gcc -DPOP_BUILD_DATA=ON
cmake --build --preset linux-gcc --target data_driver
ctest --preset linux-gcc -R '^DataRecovery$' -V
```

CTest 每次创建独立 Docker 私网、随机凭据和临时 Router 配置，运行 `mysql:8.0.44`、`rabbitmq:3.12.1` 和三个 `redis:7.2.5` 节点。Router 的回环端口转发至样例主库。三个 Redis 主节点分配全部槽位。停止 Broker、清空缓存和释放数据卷都限定在本次实例内。

| 故障时序 | 驱动检查的结果 |
| --- | --- |
| 数据库持有聚合 42 的行锁 | 从 InnoDB 确认两个写事务进入锁等待，同进程的 Space 101 继续提交输入。解锁前已过截止时间的操作回滚，原号查不到持久效果。 |
| 提交后丢弃操作 101 的首个回包，并并发重试操作 102 | 按 101 查回版本 1、值 3；两次 102 都取得版本 2、值 8。同一操作号携带不同增量会被拒绝。 |
| 停止 RabbitMQ 期间写入，恢复后重复投递同一事件 | 两笔事件从 Outbox 补发，Inbox 只落两次不同的业务效果；无路由发布可识别，反复投影失败的事件可从隔离队列按原身份恢复。 |
| 清空三节点 Redis，在重建期间再提交一次 | 从旧水位可重新安装版本 2，再从持久历史追至版本 3、值 12。迟到的版本 1 无法改写版本 3。 |

隔离验收使用单个 MySQL 主库和单成员 RabbitMQ quorum queue。多机多数派切换、生产分片部署、账号与资产流程各有后续里程碑。[验收编排](../tests/data_recovery.py) · [驱动与断言](../tests/data_driver.cc) · [总计划](../DevelopPlan.md)

2026-11-27 在 Ubuntu 22.04 的既有 `linux-gcc` 构建目录编译三个数据模块和驱动，网络依赖沿用系统包，构建时生成 ODB 映射。MySQL Router 8.0.46 将验收连接转至 MySQL 8.0.44。最终一次 `DataRecovery` 用时 52.44 秒，通过上述场景。两笔数据库事务等待行锁期间，Space 101 的观测 Tick 为 3，新输入在 Tick 5 获得权威回执。
