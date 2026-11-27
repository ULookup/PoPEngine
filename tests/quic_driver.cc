// 无界面 QUIC 协议驱动，用真实数据报请求场景执行回执或普通状态。
#include <arpa/inet.h>
#include <openssl/x509v3.h>

#include <array>
#include <charconv>
#include <chrono>
#include <future>
#include <iostream>
#include <string>
#include <string_view>
#include <vector>

#include "engine/net/quic_context.h"

namespace {
using Bytes = std::vector<std::uint8_t>;
using namespace std::chrono_literals;

// 在相同驱动上选择需要检查的传输行为。
enum class Mode {
  Receipt,    // 在单向可靠流读取权威回执。
  State,      // 在数据报读取可替换的普通状态。
  NoDatagram  // 握手不宣告数据报能力，验证兼容连接选择。
};

// 尚未发送输入，上层可用原始请求启动 TCP/TLS 兼容连接。
struct UseTcp : std::runtime_error {
  // 标记已知可安全重试的传输选择结果。
  UseTcp() : std::runtime_error("QUIC unavailable; use TCP/TLS") {}
};

// MsQuic 校验信任链，此处对照连接目标匹配证书中的 DNS 或 IP SAN。
bool MatchesHost(const QUIC_CERTIFICATE* certificate, const std::string& host) {
  const auto* der = static_cast<const QUIC_BUFFER*>(certificate);
  if (!der || !der->Buffer) {
    return false;
  }
  const unsigned char* bytes = der->Buffer;
  std::unique_ptr<X509, decltype(&X509_free)> cert(
      d2i_X509(nullptr, &bytes, der->Length), X509_free);
  if (!cert) {
    return false;
  }
  std::array<unsigned char, sizeof(in6_addr)> address{};
  if (inet_pton(AF_INET, host.c_str(), address.data()) == 1 ||
      inet_pton(AF_INET6, host.c_str(), address.data()) == 1) {
    return X509_check_ip_asc(cert.get(), host.c_str(), 0) == 1;
  }
  return X509_check_host(cert.get(), host.c_str(), host.size(),
                         X509_CHECK_FLAG_NEVER_CHECK_SUBJECT, nullptr) == 1;
}

// 通过标准输入接收有界二进制信封，临时会话凭据始终留在数据管道内。
Bytes ReadRequest() {
  Bytes bytes(pop::quic::MAX_DATAGRAM_BYTES + 1);
  std::cin.read(reinterpret_cast<char*>(bytes.data()), bytes.size());
  const auto size = std::cin.gcount();
  if (!std::cin.eof() || size == 0) {
    throw std::invalid_argument("invalid QUIC request length");
  }
  bytes.resize(size);
  return bytes;
}

// 用一个连接校验服务器证书并等待应用回执，超时后由连接所有者终止。
class Driver {
 public:
  // 配置受信 CA 并准备一份保持到连接关闭的数据报缓冲。
  Driver(const char* ca, Bytes input, Mode mode)
      : m_mode(mode),
        m_input(std::move(input)),
        m_buffer{static_cast<std::uint32_t>(m_input.size()), m_input.data()} {
    QUIC_CREDENTIAL_CONFIG credentials{};
    credentials.Type = QUIC_CREDENTIAL_TYPE_NONE;
    credentials.Flags = QUIC_CREDENTIAL_FLAG_CLIENT |
                        QUIC_CREDENTIAL_FLAG_SET_CA_CERTIFICATE_FILE |
                        QUIC_CREDENTIAL_FLAG_INDICATE_CERTIFICATE_RECEIVED |
                        QUIC_CREDENTIAL_FLAG_USE_PORTABLE_CERTIFICATES;
    credentials.CaCertificateFile = ca;
    m_context = std::make_unique<pop::quic::Context>(credentials, 3s,
                                                     mode != Mode::NoDatagram);
  }

  // 连接关闭与所有回调完成后，才释放注册和读写缓冲。
  ~Driver() {
    if (m_started) {
      m_context->api()->ConnectionShutdown(
          m_connection.get(), QUIC_CONNECTION_SHUTDOWN_FLAG_NONE, 0);
      m_stopped.get_future().wait_for(1s);
    }
    m_connection.reset();
  }

  // 真实握手后发一条连续输入，仅在收到完整可靠流回执时返回。
  Bytes run(const char* host, std::uint16_t port) {
    m_host = host;
    auto answer = m_reply.get_future();
    HQUIC conn = nullptr;
    auto* api = m_context->api();
    pop::quic::Check(api->ConnectionOpen(m_context->registration(),
                                         OnConnection, this, &conn),
                     "ConnectionOpen");
    m_connection = pop::quic::Handle(conn, api->ConnectionClose);
    pop::quic::Check(
        api->ConnectionStart(conn, m_context->configuration(),
                             QUIC_ADDRESS_FAMILY_UNSPEC, host, port),
        "ConnectionStart");
    m_started = true;
    if (answer.wait_for(5s) != std::future_status::ready) {
      throw std::runtime_error("timeout waiting for Scene receipt");
    }
    return answer.get();
  }

 private:
  // 每个连接的事件串行处理，握手与能力协商都完成才可发包。
  static QUIC_STATUS QUIC_API OnConnection(HQUIC conn, void* context,
                                           QUIC_CONNECTION_EVENT* event) {
    auto& driver = *static_cast<Driver*>(context);
    try {
      switch (event->Type) {
        case QUIC_CONNECTION_EVENT_PEER_CERTIFICATE_RECEIVED:
          if (!MatchesHost(event->PEER_CERTIFICATE_RECEIVED.Certificate,
                           driver.m_host)) {
            return QUIC_STATUS_BAD_CERTIFICATE;
          }
          break;
        case QUIC_CONNECTION_EVENT_CONNECTED: {
          BOOLEAN enabled = FALSE;
          std::uint32_t length = sizeof(enabled);
          pop::quic::Check(driver.m_context->api()->GetParam(
                               conn, QUIC_PARAM_CONN_DATAGRAM_SEND_ENABLED,
                               &length, &enabled),
                           "DATAGRAM capability");
          if (!enabled) {
            throw UseTcp();
          }
          driver.m_connected = true;
          driver.trySend(conn);
          break;
        }
        case QUIC_CONNECTION_EVENT_DATAGRAM_STATE_CHANGED:
          driver.m_maxDatagram =
              event->DATAGRAM_STATE_CHANGED.SendEnabled
                  ? event->DATAGRAM_STATE_CHANGED.MaxSendLength
                  : 0;
          driver.trySend(conn);
          break;
        case QUIC_CONNECTION_EVENT_PEER_STREAM_STARTED:
          driver.m_context->api()->SetCallbackHandler(
              event->PEER_STREAM_STARTED.Stream,
              reinterpret_cast<void*>(OnReceipt), &driver);
          break;
        case QUIC_CONNECTION_EVENT_DATAGRAM_RECEIVED: {
          if (driver.m_done) {
            break;
          }
          const auto& data = *event->DATAGRAM_RECEIVED.Buffer;
          if (driver.m_mode != Mode::State || !data.Length ||
              data.Length > pop::quic::MAX_DATAGRAM_BYTES) {
            throw std::runtime_error("unexpected state datagram");
          }
          driver.m_reply.set_value(
              Bytes(data.Buffer, data.Buffer + data.Length));
          driver.m_done = true;
          break;
        }
        case QUIC_CONNECTION_EVENT_SHUTDOWN_INITIATED_BY_PEER:
          if (!driver.m_sent && driver.m_connected &&
              event->SHUTDOWN_INITIATED_BY_PEER.ErrorCode ==
                  pop::quic::DATAGRAM_REQUIRED) {
            throw UseTcp();
          }
          break;
        case QUIC_CONNECTION_EVENT_SHUTDOWN_INITIATED_BY_TRANSPORT: {
          const auto status = event->SHUTDOWN_INITIATED_BY_TRANSPORT.Status;
          if (!driver.m_connected && !driver.m_sent &&
              (status == QUIC_STATUS_UNREACHABLE ||
               status == QUIC_STATUS_CONNECTION_REFUSED ||
               status == QUIC_STATUS_CONNECTION_TIMEOUT)) {
            throw UseTcp();
          }
          throw std::runtime_error("QUIC transport closed: " +
                                   std::to_string(status));
        }
        case QUIC_CONNECTION_EVENT_SHUTDOWN_COMPLETE:
          driver.m_stopped.set_value();
          throw std::runtime_error("QUIC closed before a Scene receipt");
        default:
          break;
      }
    } catch (...) {
      driver.fail();
    }
    return QUIC_STATUS_SUCCESS;
  }

  // 在能力更新以后校验路径预算；发送完成只释放传输层借用。
  void trySend(HQUIC conn) {
    if (!m_connected || !m_maxDatagram || m_sent ||
        m_mode == Mode::NoDatagram) {
      return;
    }
    if (m_input.size() > m_maxDatagram) {
      throw std::runtime_error("request exceeds negotiated DATAGRAM length");
    }
    pop::quic::Check(m_context->api()->DatagramSend(
                         conn, &m_buffer, 1, QUIC_SEND_FLAG_NONE, nullptr),
                     "DatagramSend");
    m_sent = true;
  }

  // 可靠流可能分多次回调，按网络序长度前缀接收一个完整回执。
  static QUIC_STATUS QUIC_API OnReceipt(HQUIC stream, void* context,
                                        QUIC_STREAM_EVENT* event) {
    auto& driver = *static_cast<Driver*>(context);
    if (event->Type == QUIC_STREAM_EVENT_SHUTDOWN_COMPLETE) {
      driver.m_context->api()->StreamClose(stream);
      return QUIC_STATUS_SUCCESS;
    }
    if (event->Type == QUIC_STREAM_EVENT_RECEIVE && !driver.m_done) {
      try {
        if (driver.m_mode != Mode::Receipt) {
          throw std::runtime_error(
              "ordinary state arrived on a reliable stream");
        }
        if (event->RECEIVE.TotalBufferLength >
            pop::MAX_REALTIME_FRAME_BYTES + 4 - driver.m_received.size()) {
          throw std::runtime_error("QUIC receipt too large");
        }
        for (unsigned i = 0; i < event->RECEIVE.BufferCount; ++i) {
          const auto& buffer = event->RECEIVE.Buffers[i];
          driver.m_received.insert(driver.m_received.end(), buffer.Buffer,
                                   buffer.Buffer + buffer.Length);
        }
        driver.readReceipt();
      } catch (...) {
        driver.fail();
      }
    }
    return QUIC_STATUS_SUCCESS;
  }

  // 用已收到的前缀确定后续长度，完整时一次发布应用载荷。
  void readReceipt() {
    if (m_received.size() < 4) {
      return;
    }
    std::uint32_t size = 0;
    for (unsigned i = 0; i < 4; ++i) {
      size = (size << 8) | m_received[i];
    }
    if (!size || size > pop::MAX_REALTIME_FRAME_BYTES ||
        m_received.size() > size + 4) {
      throw std::runtime_error("invalid QUIC receipt frame");
    }
    if (m_received.size() == size + 4) {
      m_reply.set_value(Bytes(m_received.begin() + 4, m_received.end()));
      m_done = true;
    }
  }

  // 将首个网络或协议错误交给等待方。
  void fail() {
    if (!m_done) {
      m_reply.set_exception(std::current_exception());
      m_done = true;
    }
  }

  Mode m_mode;                    // 当前用例选择的应答通道。
  std::string m_host;             // 握手要求的证书名称或 IP。
  Bytes m_input;                  // 在途数据报始终借用此内存。
  QUIC_BUFFER m_buffer;           // MsQuic 的数据报视图。
  Bytes m_received;               // 分片收到的可靠帧。
  std::size_t m_maxDatagram = 0;  // 当前路径可发送的最大载荷。
  bool m_connected = false;       // 已通过证书校验并完成握手。
  bool m_sent = false;            // 已提交数据报。
  bool m_done = false;            // 已报告回执或异常。
  std::promise<Bytes> m_reply;    // 完整应用结果，仅由连接回调写入。
  bool m_started = false;         // 主线程成功发起连接后，析构时等待关闭。
  std::promise<void> m_stopped;   // 网络线程确认该连接已停止。

  std::unique_ptr<pop::quic::Context> m_context;     // 客户端配置和注册。
  pop::quic::Handle m_connection{nullptr, nullptr};  // 当前测试连接。
};
}  // namespace

// 运行一份已编码请求，将完整二进制回执写入标准输出。
int main(int argc, char** argv) {
  try {
    if (argc < 4 || argc > 5) {
      throw std::invalid_argument(
          "usage: quic_driver HOST PORT CA [datagram|no-datagram]");
    }
    const std::string_view port_text = argv[2];
    std::uint16_t port = 0;
    const auto [end, error] = std::from_chars(
        port_text.data(), port_text.data() + port_text.size(), port);
    if (error != std::errc{} || end != port_text.data() + port_text.size() ||
        !port) {
      throw std::invalid_argument("invalid port");
    }
    Mode mode = Mode::Receipt;
    if (argc == 5) {
      std::string_view option(argv[4]);
      if (option == "datagram") {
        mode = Mode::State;
      } else if (option == "no-datagram") {
        mode = Mode::NoDatagram;
      } else {
        throw std::invalid_argument("unknown QUIC driver mode");
      }
    }
    Driver driver(argv[3], ReadRequest(), mode);
    const auto reply = driver.run(argv[1], port);
    std::cout.write(reinterpret_cast<const char*>(reply.data()), reply.size());
    return 0;
  } catch (const UseTcp& reason) {
    std::cerr << reason.what() << '\n';
    return 3;
  } catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return 1;
  }
}
