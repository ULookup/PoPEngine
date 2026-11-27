// 网络适配器交换有界值消息。发送函数可由其他线程调用，返回当次入队结果。
#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <vector>

#include "engine/core/types.h"

namespace pop {

// 普通实时信封的最大字节数，流传输附加的长度前缀单独计量。
inline constexpr std::size_t MAX_REALTIME_FRAME_BYTES = 64 * 1024;

// TCP 和 QUIC 可靠流均以四字节网络序长度划分信封。
inline constexpr std::size_t STREAM_PREFIX_BYTES = 4;

// 一份完整的应用帧，回调接管缓冲区后可跨线程移交。
using WireBytes = std::vector<std::uint8_t>;

// 为已经过长度和队列预算检查的信封添加流前缀，只分配最终需要的缓冲区。
inline WireBytes EncodeStreamFrame(const WireBytes& payload) {
  WireBytes frame(STREAM_PREFIX_BYTES + payload.size());
  auto length = static_cast<std::uint32_t>(payload.size());
  for (std::size_t i = STREAM_PREFIX_BYTES; i > 0; --i) {
    frame[i - 1] = static_cast<std::uint8_t>(length);
    length >>= 8;
  }
  std::copy(payload.begin(), payload.end(), frame.begin() + STREAM_PREFIX_BYTES);
  return frame;
}

// 发送队列的即时结果，业务终态由对端权威服务单独返回。
enum class SendStatus {
  Queued,   // 已入队。
  Merged,   // 已替换同键的待发送状态。
  Full,     // 已达到条数或字节上限。
  Closed,   // 当前没有可用连接。
  TooLarge  // 该传输无法承载此帧。
};

// 返回本连接的投递结果。空键用于可靠帧，同键的待发状态可以合并。
using SendFrame = std::function<SendStatus(WireBytes, std::optional<Id>)>;
// 收到完整帧后交付数据和本连接的回送入口。
using ReceiveFrame = std::function<void(WireBytes, SendFrame)>;

// 服务监听或客户端拨号使用的网络端点。
struct Endpoint {
  std::string m_host;    // 监听地址或对端主机。
  std::uint16_t m_port;  // TCP 或 UDP 端口。
};

// PEM 文件仅保存路径，私钥由传输库在启动时加载。
struct TlsFiles {
  std::string m_ca;           // 受信任对端的 CA 文件。
  std::string m_certificate;  // 本端证书链。
  std::string m_key;          // 本端证书的私钥。
  std::string m_peerName;     // 期望对端证书中的身份。
};

// 网络预算由配置给定；队列容量按每条连接计量。
struct NetLimits {
  std::size_t m_maxConnections = 64;  // 同时服务的最大连接数。
  std::size_t m_queueEntries = 64;    // 每连接最多待发送的应用帧数。
  std::size_t m_queueBytes = 262144;  // 每连接发送队列可占用的字节数。
  Milliseconds m_timeout{3000};       // I/O 超时和上层请求等待预算。
};

// 进程拥有监听服务；析构时停止接收并等待其回调退出。
class NetworkListener {
 public:
  // 回收网络运行时及仍在处理的连接。
  virtual ~NetworkListener() = default;
};

}  // namespace pop
