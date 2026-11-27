// 描述各进程实际装配的网络入口与可选的回环验收身份。
#pragma once

#include <optional>
#include <string>
#include <variant>

#include "engine/net/transport.h"

namespace pop {

// 仅在显式配置的回环验收环境中预置一个实体会话。
struct FixtureBinding {
  SpaceId m_space;          // 目标 Space，初始静态承载代次为 1。
  EntityBinding m_binding;  // 来源实体与会话身份。
};

// 将隔离驱动的凭据映射到可信实体和当次 Worker 进程。
struct FixtureRoute {
  FixtureBinding m_target;  // 当前会话有权投递的目标。
  std::string m_worker;     // 已就绪 Worker 报告的实例标识。
  std::string m_tokenFile;  // 本地验收凭据路径，凭据不写入日志。
};

// 网关在同一端口监听 UDP/QUIC 和 TCP/TLS，另以客户端身份连接 Worker。
struct GatewayNetwork {
  Endpoint m_ingress;                   // 客户端入口。
  Endpoint m_scene;                     // Worker 的实时通道。
  TlsFiles m_publicTls;                 // 公网服务端证书。
  TlsFiles m_internalTls;               // 网关身份及受信 Worker。
  std::optional<FixtureRoute> m_route;  // 仅限回环验收的会话提供者。
};

// Scene 仅从已验证的网关接收内部批次。
struct SceneNetwork {
  Endpoint m_endpoint;                      // 实时内网监听地址。
  TlsFiles m_tls;                           // Worker 身份及受信网关。
  std::optional<FixtureBinding> m_fixture;  // 可选的验收实体绑定。
};

// Platform 接收由 Nginx 转发的内部 HTTP 请求。
struct PlatformNetwork {
  Endpoint m_http;  // 内部 Beast 监听地址。
};

// Control 监听受双向认证保护的 RPC。
struct ControlNetwork {
  Endpoint m_rpc;  // gRPC 地址。
  TlsFiles m_tls;  // Control 的凭据和调用方信任配置。
};

// 每个角色只启用对应服务；旧配置默认空载运行。
struct NetworkConfig {
  NetLimits m_limits;  // 各适配器应用的共享预算。
  std::variant<std::monostate, GatewayNetwork, SceneNetwork, PlatformNetwork,
               ControlNetwork>
      m_service;  // 本进程的网络职责。
};

}  // namespace pop
