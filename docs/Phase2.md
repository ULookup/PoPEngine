# 第二阶段 · 网络传输与进程通信

本方案说明四个进程怎样接入传输、一次输入何时算执行完成，以及怎样用隔离的协议驱动复现这些行为。设计依据是[架构第 7 节](Architecture.md#7-通信架构)和[TDD 第 5 节](TDD.md#5-通信与消息协议)。

## 从输入到权威回执

1. 协议驱动经 QUIC DATAGRAM 或 TCP/TLS 把完整控制状态和 `input_seq` 发给 Gateway。两种入口共用 Protobuf 信封和身份校验。隔离样例的目标 Space、实体、会话代次和 Worker 实例均由 Gateway 从预置绑定取得。
2. Gateway 为转发分配请求号，在有界表中保留原连接的回送句柄。它把同一 Space、承载代次和 Worker 的输入收为一批，经过 mTLS 发往 Scene。批处理和场景邮箱均按实体绑定识别旧序号。同一绑定的待发输入被新版替换时，原请求收到 `SUPERSEDED`。
3. Worker 检查本次启动的实例 ID、静态承载代次和截止时间，再通过 `GameWorker::postInput` 入队。网关收包时给输入 250 ms 有效期，内部消息携带 Unix 毫秒截止时间。Worker 在入口换算为本机单调时钟，剩余有效期最多 250 ms。跨主机部署需要对时。
4. Scene 在所属分片检查绑定与序号，采纳输入、推进该步并发布快照。随后回送 `APPLIED`、Space、序号及 `server_tick`。投递前满载返回 `BUSY`，错误归属返回 `STALE_ROUTE`。已转发的请求等不到回执时，Gateway 返回 `UNKNOWN`。套接字写完成仅归还发送预算。

输入和执行回执走关键消息连接，已发布状态走另一条 mTLS 长连接。普通状态的隔离样例读取 Scene 已发布的 Tick、实体数和输入计数，向 QUIC 连接发送 DATAGRAM，向 TCP 连接发送可覆盖帧。实体基线、增量和可见性序号属于阶段 4。[实时消息定义](../proto/realtime.proto)

## 连接、预算与配置

每个 TCP 帧以四字节网络序长度开头，长度仅计信封，取值为 1 至 65536 字节。QUIC 使用 ALPN `pop-rt-1`，数据报发送上限取 1000 字节与 MsQuic 协商上限的较小值。可靠执行回执在单向流中使用长度前缀。协议驱动遇到未协商 DATAGRAM 的连接时，经 TCP/TLS 兼容入口提交同一输入。

进程配置的 `network` 段按角色装配。端点写作 `{host, port}`，证书字段均填写 PEM 文件路径。内部 TLS 的 `peer_name` 是期望出现在对端证书 SAN 中的服务身份。

| 角色 | 配置 | 启动结果 |
| --- | --- | --- |
| Gateway | `ingress`、`scene`、`public_tls`、`tls` | 同一端口监听 UDP 和 TCP，使用网关证书拨号到 Worker。 |
| GameWorker | `scene`、`tls` | 监听内部实时通道，要求已验证的网关证书。 |
| Platform | `http` | Boost.Beast 提供 `GET /healthz`，由 Nginx 终止 HTTPS 并转发。 |
| Control | `rpc`、`tls` | gRPC C++ 提供 `WorldCoordinator` 的标准 Health Check 和 Watch，校验调用方证书。 |

可选的 `network.limits` 包含 `max_connections`、`queue_entries`、`queue_bytes` 和 `timeout_ms`。默认分别为 64、64、262144 字节和 3000 ms。TLS 的在途写占用发送预算。普通状态可按键替换，可靠回执积压超限时关闭该连接，出站内网拨号失败后按带抖动的退避重连。gRPC 使用自身的运行时，限制每连接并发流数和整个服务的内存预算。

隔离用例为每次运行生成临时 CA、不同角色的证书和 32 字节随机凭据。只有显式装配 `isolated_fixture` 的回环配置才提供样例身份。用例从 Worker 的就绪日志取得本次实例 ID，再启动 Gateway。旧 YAML 可继续空载启动；账号登录和正式玩家会话按计划在阶段 6 接入。[可运行的配置样例](../tests/realtime_integration.py) · [Nginx 模板](../deploy/nginx/platform.conf.in)

## 本地验收

按 [README](../README.md#构建) 构建后运行以下命令。实时用例打印输入序号、目标 Space 和实际提交 Tick，具体 Tick 随进程启动时刻变化。

```bash
ctest --preset linux-gcc -R '^(TransportQueue|InputBatching|RealtimeIngress|PlatformHttpsHealth|ControlRpcDeadline)$' -V
```

HTTP 用例启动真实 Nginx 和 Platform；机器上缺少 Nginx 时，CTest 将该项显示为跳过。RPC 用例在同一 HTTP/2 连接上限制并发数，观察 Watch 到期或取消后新的 Check 能取得服务结果。上述用例覆盖本地回环行为，公网时延、跨机故障和玩家容量有各自的后续验收。

2026-11-24 的 Ubuntu 22.04 回环验收使用 MsQuic 2.4.8、Boost 1.74、OpenSSL 3.0、Protobuf 3.12、gRPC 1.30 和 Nginx 1.18。四个正式角色均完成构建，12 项 CTest 全部通过。一次记录中，Space 101 的 TCP/TLS 输入序号 1 在 Tick 4 提交，QUIC 输入序号 2 在 Tick 7 提交。

2026-11-27 使用 vcpkg 基线中的 Boost 1.92、OpenSSL 3.6.5、Protobuf 36.2 和 gRPC 1.84 编译了同样的四个进程。Linux 链接配置将应用静态 OpenSSL 的符号保留在可执行文件内部，MsQuic 共享库使用其配套的系统 libcrypto。12 项 CTest 全部通过，Space 101 的 TCP/TLS 和 QUIC 输入分别在 Tick 5 和 Tick 9 提交。此次复用了中断前的 vcpkg 构建目录，并从该目录补编了项目实际使用的 gRPC C++ 库和代码生成器；完整依赖包的从零安装未复验。
