// 为公网 TCP/TLS 兼容入口和内网 mTLS 长连接提供有界收发接口。
// 收包回调在 I/O 线程处理完整载荷，回包句柄可跨线程使用。
#pragma once

#include <cstdint>
#include <memory>
#include <optional>

#include "engine/net/transport.h"

namespace pop {

// 监听指定地址。配置 CA 和预期对端名时同时验证客户端证书和身份。
std::unique_ptr<NetworkListener> StartTlsListener(Endpoint endpoint,
                                                  TlsFiles files,
                                                  NetLimits limits,
                                                  ReceiveFrame receive);

// 持有一条带重连退避的出站连接。销毁后停用回调并回收 I/O 线程。
class TlsClient {
 public:
  // 结束连接，不把旧连接上的待写消息重放到新会话。
  virtual ~TlsClient() = default;

  // 向已握手的连接投递帧；未连接返回 Closed，满载返回 Full 并继续发送已受理帧。
  virtual SendStatus send(
      WireBytes payload,
      std::optional<std::uint64_t> replace_key = std::nullopt) = 0;
};

// 异步解析、建连并验证服务端证书；提供本机证书和私钥时启用双向认证。
std::unique_ptr<TlsClient> StartTlsClient(Endpoint endpoint, TlsFiles files,
                                          NetLimits limits,
                                          ReceiveFrame receive);

}  // namespace pop
