// MsQuic 回调将完整输入数据报交给 Gateway；每连接独立限制在途和待发字节。
#include "engine/net/quic_transport.h"

#include <msquic.h>

#include <algorithm>
#include <atomic>
#include <iterator>
#include <list>
#include <mutex>
#include <stdexcept>
#include <utility>

#include "engine/net/quic_context.h"

namespace pop {
namespace {

// 保存一个待发包；MsQuic 完成发送前缓冲地址保持稳定。
struct Packet {
  std::optional<Id> m_key;  // 空键表示可靠回执。
  WireBytes m_bytes;        // 自有的线格式字节，可靠帧包含长度前缀。
  QUIC_BUFFER m_buffer{};   // 指向上述字节，直到发送回调才释放。
};

// 各交付类别独立推进，一份待确认的数据报不会延后可靠流回执。
struct SendLane {
  std::list<Packet> m_queue;  // 队首先提交给 MsQuic，地址在回调前保持稳定。
  bool m_sending = false;     // 当前队首的发送缓冲已由传输层借用。
};

// 表示一条公网连接；回送入口只持有弱引用，关闭后无法误投给重连会话。
class Peer : public std::enable_shared_from_this<Peer> {
 public:
  // 借用所属注册的 API 和接收入口，连接预算在终止回调时归还。
  Peer(const QUIC_API_TABLE* api, HQUIC connection, const NetLimits& limits,
       const ReceiveFrame& receive, std::atomic_size_t& active)
      : m_api(api),
        m_connection(connection),
        m_limits(limits),
        m_receive(receive),
        m_active(active) {}

  // 每条新连接获得一份强引用，等待 SHUTDOWN_COMPLETE 释放。
  static QUIC_STATUS QUIC_API OnConnection(HQUIC connection, void* context,
                                           QUIC_CONNECTION_EVENT* event) {
    auto* owner = static_cast<std::shared_ptr<Peer>*>(context);
    auto& peer = **owner;
    if (event->Type == QUIC_CONNECTION_EVENT_SHUTDOWN_COMPLETE) {
      {
        std::lock_guard lock(peer.m_mutex);
        peer.m_connection = nullptr;
      }
      --peer.m_active;
      peer.m_api->ConnectionClose(connection);
      delete owner;
      return QUIC_STATUS_SUCCESS;
    }

    try {
      peer.onEvent(*event);
    } catch (...) {
      peer.m_api->ConnectionShutdown(connection,
                                     QUIC_CONNECTION_SHUTDOWN_FLAG_NONE, 1);
    }
    return QUIC_STATUS_SUCCESS;
  }

 private:
  // 在串行的连接回调中处理协商、输入和数据报发送完成。
  void onEvent(const QUIC_CONNECTION_EVENT& event) {
    switch (event.Type) {
      case QUIC_CONNECTION_EVENT_CONNECTED: {
        BOOLEAN enabled = FALSE;
        std::uint32_t length = sizeof(enabled);
        quic::Check(
            m_api->GetParam(m_connection, QUIC_PARAM_CONN_DATAGRAM_SEND_ENABLED,
                            &length, &enabled),
            "DATAGRAM capability");
        if (!enabled) {
          m_api->ConnectionShutdown(m_connection,
                                    QUIC_CONNECTION_SHUTDOWN_FLAG_NONE,
                                    quic::DATAGRAM_REQUIRED);
          break;
        }
        HQUIC stream = nullptr;
        quic::Check(m_api->StreamOpen(m_connection,
                                      QUIC_STREAM_OPEN_FLAG_UNIDIRECTIONAL,
                                      OnStream, this, &stream),
                    "StreamOpen");
        const auto status =
            m_api->StreamStart(stream, QUIC_STREAM_START_FLAG_SHUTDOWN_ON_FAIL);
        if (QUIC_FAILED(status)) {
          m_api->StreamClose(stream);
          quic::Check(status, "StreamStart");
        }
        std::lock_guard lock(m_mutex);
        m_stream = stream;
        break;
      }
      case QUIC_CONNECTION_EVENT_DATAGRAM_STATE_CHANGED: {
        std::lock_guard lock(m_mutex);
        m_maxDatagram = event.DATAGRAM_STATE_CHANGED.SendEnabled
                            ? std::min<std::size_t>(
                                  quic::MAX_DATAGRAM_BYTES,
                                  event.DATAGRAM_STATE_CHANGED.MaxSendLength)
                            : 0;
        break;
      }
      case QUIC_CONNECTION_EVENT_DATAGRAM_RECEIVED: {
        const auto& buffer = *event.DATAGRAM_RECEIVED.Buffer;
        if (!m_stream || buffer.Length == 0 ||
            buffer.Length > quic::MAX_DATAGRAM_BYTES ||
            (event.DATAGRAM_RECEIVED.Flags & QUIC_RECEIVE_FLAG_0_RTT)) {
          abortConnection();
          break;
        }
        m_receive(
            WireBytes(buffer.Buffer, buffer.Buffer + buffer.Length),
            [peer = weak_from_this()](WireBytes bytes, std::optional<Id> key) {
              auto self = peer.lock();
              return self ? self->send(bytes, key) : SendStatus::Closed;
            });
        break;
      }
      case QUIC_CONNECTION_EVENT_DATAGRAM_SEND_STATE_CHANGED:
        if (QUIC_DATAGRAM_SEND_STATE_IS_FINAL(
                event.DATAGRAM_SEND_STATE_CHANGED.State)) {
          complete(m_states, event.DATAGRAM_SEND_STATE_CHANGED.ClientContext);
        }
        break;
      default:
        break;
    }
  }

  // 可靠流只承载服务器回执。发送完成后才复用连接级字节预算。
  static QUIC_STATUS QUIC_API OnStream(HQUIC stream, void* context,
                                       QUIC_STREAM_EVENT* event) {
    auto& peer = *static_cast<Peer*>(context);
    if (event->Type == QUIC_STREAM_EVENT_SEND_COMPLETE) {
      peer.complete(peer.m_replies, event->SEND_COMPLETE.ClientContext,
                    event->SEND_COMPLETE.Canceled);
    } else if (event->Type == QUIC_STREAM_EVENT_PEER_RECEIVE_ABORTED) {
      peer.abortConnection();
    } else if (event->Type == QUIC_STREAM_EVENT_SHUTDOWN_COMPLETE) {
      {
        std::lock_guard lock(peer.m_mutex);
        peer.m_stream = nullptr;
        peer.abortConnection();
      }
      peer.m_api->StreamClose(stream);
    }
    return QUIC_STATUS_SUCCESS;
  }

  // 仅在 MsQuic 串行回调或持锁的跨线程发送中请求结束本连接。
  void abortConnection() {
    m_api->ConnectionShutdown(m_connection, QUIC_CONNECTION_SHUTDOWN_FLAG_NONE,
                              1);
  }

  // 在预算内接收回执或可覆盖状态，只合并尚未交给 MsQuic 的同键包。
  SendStatus send(const WireBytes& bytes, std::optional<Id> key) {
    std::lock_guard lock(m_mutex);
    if (!m_connection || !m_stream || m_closed) {
      return SendStatus::Closed;
    }
    if (bytes.empty() ||
        bytes.size() > (key ? m_maxDatagram : MAX_REALTIME_FRAME_BYTES)) {
      if (!key) {
        m_closed = true;
        abortConnection();
      }
      return SendStatus::TooLarge;
    }

    const auto size = bytes.size() + (key ? 0 : STREAM_PREFIX_BYTES);
    auto& lane = key ? m_states : m_replies;
    auto& queue = lane.m_queue;
    auto pending = queue.end();
    if (key) {
      const auto first =
          lane.m_sending ? std::next(queue.begin()) : queue.begin();
      pending = std::find_if(first, queue.end(), [key](const Packet& item) {
        return item.m_key == key;
      });
    }
    const bool merge = pending != queue.end();
    const auto released = merge ? pending->m_bytes.size() : 0;
    if ((!merge && m_states.m_queue.size() + m_replies.m_queue.size() >=
                       m_limits.m_queueEntries) ||
        size > m_limits.m_queueBytes - (m_bytes - released)) {
      if (!key) {
        m_closed = true;
        abortConnection();
      }
      return SendStatus::Full;
    }

    WireBytes packet = key ? bytes : EncodeStreamFrame(bytes);
    if (merge) {
      pending->m_bytes = std::move(packet);
    } else {
      queue.push_back({key, std::move(packet), {}});
    }
    m_bytes = m_bytes - released + size;
    const auto status = pump(lane);
    return status == SendStatus::Queued && merge ? SendStatus::Merged : status;
  }

  // 持锁提交一项异步发送，网络栈借用队首直到相应发送完成回调。
  SendStatus pump(SendLane& lane) {
    if (m_closed) {
      return SendStatus::Closed;
    }
    if (lane.m_sending) {
      return SendStatus::Queued;
    }
    // 路径载荷预算可能在排队期间收紧，过大的普通状态等待下一次采样。
    auto& queue = lane.m_queue;
    while (!queue.empty()) {
      auto& packet = queue.front();
      packet.m_buffer = {static_cast<std::uint32_t>(packet.m_bytes.size()),
                         packet.m_bytes.data()};
      QUIC_STATUS status = QUIC_STATUS_INVALID_PARAMETER;
      if (!packet.m_key) {
        status = m_api->StreamSend(m_stream, &packet.m_buffer, 1,
                                   QUIC_SEND_FLAG_NONE, &packet);
      } else if (packet.m_bytes.size() <= m_maxDatagram) {
        status = m_api->DatagramSend(m_connection, &packet.m_buffer, 1,
                                     QUIC_SEND_FLAG_NONE, &packet);
      }
      if (QUIC_SUCCEEDED(status)) {
        lane.m_sending = true;
        return SendStatus::Queued;
      }
      if (!packet.m_key || status != QUIC_STATUS_INVALID_PARAMETER) {
        m_closed = true;
        abortConnection();
        return SendStatus::Closed;
      }
      m_bytes -= packet.m_bytes.size();
      queue.pop_front();
    }
    return SendStatus::TooLarge;
  }

  // 回收已完成包的预算，在同一把锁内启动下一项。
  void complete(SendLane& lane, void* context, bool cancelled = false) {
    std::lock_guard lock(m_mutex);
    if (!lane.m_sending || context != &lane.m_queue.front()) {
      return;
    }
    m_bytes -= lane.m_queue.front().m_bytes.size();
    lane.m_queue.pop_front();
    lane.m_sending = false;
    if (cancelled || !m_stream) {
      m_closed = true;
      abortConnection();
    } else {
      pump(lane);
    }
  }

  const QUIC_API_TABLE* m_api;    // 注册存活期间有效的 API 表。
  HQUIC m_connection;             // 连接关闭回调将其置空。
  const NetLimits m_limits;       // 本连接收发预算。
  const ReceiveFrame& m_receive;  // 所属 Gateway 的投递回调。
  std::atomic_size_t& m_active;   // 监听器握手与已接入的连接总数。

  std::mutex m_mutex;        // 串行化跨线程入队和传输回调。
  HQUIC m_stream = nullptr;  // 发送可靠回执的单向流。
  SendLane m_replies;        // 关键回执经有序可靠流发送。
  SendLane m_states;         // 可替换状态经路径预算允许的数据报发送。
  std::size_t m_bytes = 0;   // 包含在途包的总字节数。
  // 服务器接管连接前可能已发生首次预算通知。此时由 DatagramSend 校验路径上限。
  std::size_t m_maxDatagram = quic::MAX_DATAGRAM_BYTES;
  bool m_closed = false;  // 本端已决定终止，不再发起新发送。
};

// 管理一个 UDP 监听端点和其全部公网连接。
class Listener final : public NetworkListener {
 public:
  // 装配服务器证书，完成监听后工厂才返回。
  Listener(const Endpoint& endpoint, const TlsFiles& files, NetLimits limits,
           ReceiveFrame receive)
      : m_limits(limits), m_receive(std::move(receive)) {
    QUIC_CERTIFICATE_FILE certificate{files.m_key.c_str(),
                                      files.m_certificate.c_str()};
    QUIC_CREDENTIAL_CONFIG credentials{};
    credentials.Type = QUIC_CREDENTIAL_TYPE_CERTIFICATE_FILE;
    credentials.CertificateFile = &certificate;
    m_context = std::make_unique<quic::Context>(credentials, limits.m_timeout);

    QUIC_ADDR address{};
    if (!QuicAddrFromString(endpoint.m_host.c_str(), endpoint.m_port,
                            &address)) {
      throw std::invalid_argument("QUIC listen address must be an IP literal");
    }
    HQUIC listener = nullptr;
    const auto* api = m_context->api();
    quic::Check(api->ListenerOpen(m_context->registration(), OnListener, this,
                                  &listener),
                "ListenerOpen");
    m_listener = quic::Handle(listener, api->ListenerClose);
    auto alpn = quic::Alpn();
    quic::Check(api->ListenerStart(listener, &alpn, 1, &address),
                "ListenerStart");
  }

  // 先停止接入，再由注册关闭等候全部连接回调，最终释放投递入口。
  ~Listener() override {
    m_listener.reset();
    m_context->api()->RegistrationShutdown(
        m_context->registration(), QUIC_CONNECTION_SHUTDOWN_FLAG_SILENT, 0);
    m_context.reset();
  }

 private:
  // 在连接握手开始前占用配额，成功配置后将所有权交给连接回调。
  static QUIC_STATUS QUIC_API OnListener(HQUIC, void* context,
                                         QUIC_LISTENER_EVENT* event) {
    if (event->Type != QUIC_LISTENER_EVENT_NEW_CONNECTION) {
      return QUIC_STATUS_SUCCESS;
    }
    auto& self = *static_cast<Listener*>(context);
    if (self.m_active.fetch_add(1) >= self.m_limits.m_maxConnections) {
      --self.m_active;
      return QUIC_STATUS_CONNECTION_REFUSED;
    }
    try {
      const auto* api = self.m_context->api();
      HQUIC conn = event->NEW_CONNECTION.Connection;
      auto owner =
          std::make_unique<std::shared_ptr<Peer>>(std::make_shared<Peer>(
              api, conn, self.m_limits, self.m_receive, self.m_active));
      api->SetCallbackHandler(conn, reinterpret_cast<void*>(Peer::OnConnection),
                              owner.release());
      const auto status = api->ConnectionSetConfiguration(
          conn, self.m_context->configuration());
      if (QUIC_FAILED(status)) {
        // 回调已接管上下文，配置失败也经同一关闭完成点归还句柄和配额。
        api->ConnectionShutdown(conn, QUIC_CONNECTION_SHUTDOWN_FLAG_SILENT, 1);
      }
      return QUIC_STATUS_SUCCESS;
    } catch (...) {
      --self.m_active;
      return QUIC_STATUS_OUT_OF_MEMORY;
    }
  }

  NetLimits m_limits;                         // 握手与每连接的预算。
  ReceiveFrame m_receive;                     // 所有连接共用的投递入口。
  std::atomic_size_t m_active{0};             // 含握手中与等待回收的连接。
  std::unique_ptr<quic::Context> m_context;   // 库、注册与配置。
  quic::Handle m_listener{nullptr, nullptr};  // 绑定在本地 UDP 端口的监听器。
};

}  // namespace

std::unique_ptr<NetworkListener> StartQuicListener(Endpoint endpoint,
                                                   TlsFiles files,
                                                   NetLimits limits,
                                                   ReceiveFrame receive) {
  return std::make_unique<Listener>(endpoint, files, limits,
                                    std::move(receive));
}

}  // namespace pop
