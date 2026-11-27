// 管理 MsQuic API、注册和 TLS 配置的所有权，供 Gateway 与协议驱动共用。
#pragma once

#include <msquic.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>

#include "engine/net/transport.h"

namespace pop::quic {

// MsQuic 当前发送上限还受路径协商影响。
inline constexpr std::size_t MAX_DATAGRAM_BYTES = 1000;

// 握手对端无法收发数据报时，请求驱动用兼容通道重新建连。
inline constexpr QUIC_UINT62 DATAGRAM_REQUIRED = 2;

// 将 MsQuic 同步启动错误转换为带步骤名的启动异常。
inline void Check(QUIC_STATUS status, const char* operation) {
  if (QUIC_FAILED(status)) {
    throw std::runtime_error(std::string(operation) +
                             " failed: " + std::to_string(status));
  }
}

// 为 MsQuic 提供只读的协议名；库的 QUIC_BUFFER 接口需要可写指针。
inline QUIC_BUFFER Alpn() {
  static std::uint8_t protocol[] = {'p', 'o', 'p', '-', 'r', 't', '-', '1'};
  return {sizeof(protocol), protocol};
}

// 由调用方所属运行时持有的 MsQuic 句柄和对应的库释放函数。
using Handle = std::unique_ptr<QUIC_HANDLE, void(QUIC_API*)(HQUIC)>;

// 一个端点拥有一份注册与配置，关闭注册会等待其连接回调退出。
class Context {
 public:
  // 初始化库、ALPN、数据报预算和 TLS。客户端允许服务器打开一条回执流。
  Context(const QUIC_CREDENTIAL_CONFIG& credentials,
          std::chrono::milliseconds timeout, bool receive_datagrams = true) {
    const QUIC_API_TABLE* api = nullptr;
    Check(MsQuicOpen2(&api), "MsQuicOpen2");
    m_api.reset(api);
    HQUIC registration = nullptr;
    QUIC_REGISTRATION_CONFIG config{"popengine",
                                    QUIC_EXECUTION_PROFILE_LOW_LATENCY};
    Check(api->RegistrationOpen(&config, &registration), "RegistrationOpen");
    m_registration = Handle(registration, api->RegistrationClose);

    QUIC_SETTINGS settings{};
    settings.DatagramReceiveEnabled = receive_datagrams;
    settings.IsSet.DatagramReceiveEnabled = TRUE;
    settings.SendBufferingEnabled = FALSE;
    settings.IsSet.SendBufferingEnabled = TRUE;
    settings.HandshakeIdleTimeoutMs = timeout.count();
    settings.IsSet.HandshakeIdleTimeoutMs = TRUE;
    settings.IdleTimeoutMs = timeout.count();
    settings.IsSet.IdleTimeoutMs = TRUE;
    settings.PeerUnidiStreamCount =
        (credentials.Flags & QUIC_CREDENTIAL_FLAG_CLIENT) ? 1 : 0;
    settings.IsSet.PeerUnidiStreamCount = TRUE;

    HQUIC configuration = nullptr;
    auto alpn = Alpn();
    Check(api->ConfigurationOpen(registration, &alpn, 1, &settings,
                                 sizeof(settings), nullptr, &configuration),
          "ConfigurationOpen");
    m_configuration = Handle(configuration, api->ConfigurationClose);
    Check(api->ConfigurationLoadCredential(configuration, &credentials),
          "ConfigurationLoadCredential");
  }

  // 向适配器借出 API 表，直到关闭注册后才卸载。
  const QUIC_API_TABLE* api() const { return m_api.get(); }

  // 返回用来管理本端点所有连接的注册对象。
  HQUIC registration() const { return m_registration.get(); }

  // 借出已完成证书与数据报配置的句柄。
  HQUIC configuration() const { return m_configuration.get(); }

 private:
  // 按成员逆序先释放配置、再关闭注册，最后卸载 API。
  std::unique_ptr<const QUIC_API_TABLE, decltype(&MsQuicClose)> m_api{
      nullptr, MsQuicClose};
  Handle m_registration{nullptr, nullptr};   // 端点对应的注册。
  Handle m_configuration{nullptr, nullptr};  // 连接共用的配置。
};

}  // namespace pop::quic
