// 通过 Asio 驱动 TLS 握手、定长前缀收发及退避重连。
// 每个监听器或出站通道拥有一个 I/O 线程，同一连接的套接字操作在该线程串行发起。
#include "engine/net/tls_transport.h"

#include <openssl/ssl.h>
#include <openssl/x509v3.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <chrono>
#include <functional>
#include <memory>
#include <random>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>

#include "engine/core/log.h"
#include "engine/net/send_queue.h"

namespace pop {
namespace {

namespace asio = boost::asio;
namespace ssl = asio::ssl;
using Tcp = asio::ip::tcp;
using Error = boost::system::error_code;

// 创建最低 TLS 1.2 的上下文，先校验 CA 链，再以指定 DNS 名或 IP 核对证书。
ssl::context MakeContext(const TlsFiles& files, bool server) {
  ssl::context context(server ? ssl::context::tls_server
                              : ssl::context::tls_client);
  auto* native = context.native_handle();
  if (SSL_CTX_set_min_proto_version(native, TLS1_2_VERSION) != 1) {
    throw std::runtime_error("cannot set minimum TLS version");
  }
  if (server || !files.m_certificate.empty() || !files.m_key.empty()) {
    context.use_certificate_chain_file(files.m_certificate);
    context.use_private_key_file(files.m_key, ssl::context::pem);
    if (SSL_CTX_check_private_key(native) != 1) {
      throw std::runtime_error("TLS certificate does not match its key");
    }
  }

  if (server && files.m_ca.empty() && files.m_peerName.empty()) {
    return context;
  }
  if (files.m_ca.empty() || files.m_peerName.empty() ||
      files.m_peerName.find('\0') != std::string::npos) {
    throw std::runtime_error("TLS peer verification requires CA and peer name");
  }
  context.load_verify_file(files.m_ca);
  context.set_verify_mode(server ? ssl::verify_peer |
                                       ssl::verify_fail_if_no_peer_cert
                                 : ssl::verify_peer);

  auto* parameters = SSL_CTX_get0_param(native);
  X509_VERIFY_PARAM_set_hostflags(parameters,
                                  X509_CHECK_FLAG_NEVER_CHECK_SUBJECT |
                                      X509_CHECK_FLAG_NO_PARTIAL_WILDCARDS);
  Error error;
  asio::ip::make_address(files.m_peerName, error);
  const int verified =
      error
          ? X509_VERIFY_PARAM_set1_host(parameters, files.m_peerName.data(),
                                        files.m_peerName.size())
          : X509_VERIFY_PARAM_set1_ip_asc(parameters, files.m_peerName.c_str());
  if (verified != 1) {
    throw std::runtime_error("invalid TLS peer name");
  }
  return context;
}

// 单线程事件循环提供隐式 strand，关闭方从进程主线程等待所有取消回调。
class IoLoop {
 public:
  // 取得此传输专用的执行器。
  asio::io_context& context() { return m_io; }

  // 已建立监听或发起解析之后开始处理事件。
  void start() {
    m_thread = std::thread([this] { m_io.run(); });
  }

  // 停机逻辑也在同一执行器上执行，退出后不再投递任何回调。
  void stop(std::function<void()> shutdown) {
    asio::post(m_io, [this, shutdown = std::move(shutdown)] {
      shutdown();
      m_guard.reset();
    });
    m_thread.join();
  }

 private:
  asio::io_context m_io;  // TLS 对象及计时器共用的事件循环。
  asio::executor_work_guard<asio::io_context::executor_type> m_guard{
      m_io.get_executor()};  // 显式停机前保持执行器存活。
  std::thread m_thread;      // 唯一访问连接状态的线程。
};

// 已接受或正在拨号的一条 TLS 连接。读写缓冲活到对应完成回调结束。
class Session : public std::enable_shared_from_this<Session> {
 public:
  // 从所属执行器接管套接字，并在握手成功和连接结束时通知所有者。
  Session(Tcp::socket socket, ssl::context& context, NetLimits limits,
          ReceiveFrame receive,
          std::function<void(std::shared_ptr<SendQueue>)> ready,
          std::function<void(Session*)> closed)
      : m_stream(std::move(socket), context),
        m_readTimer(m_stream.get_executor()),
        m_writeTimer(m_stream.get_executor()),
        m_timeout(limits.m_timeout),
        m_queue(std::make_shared<SendQueue>(limits)),
        m_receive(std::move(receive)),
        m_ready(std::move(ready)),
        m_closed(std::move(closed)) {}

  // 轮流尝试本次解析的地址，TCP 建连和 TLS 握手分别受截止时间约束。
  void connect(Tcp::resolver::results_type addresses, const std::string& peer) {
    Error ip_error;
    asio::ip::make_address(peer, ip_error);
    if (ip_error &&
        !SSL_set_tlsext_host_name(m_stream.native_handle(), peer.c_str())) {
      close("server_name");
      return;
    }
    arm(m_readTimer);
    asio::async_connect(
        m_stream.next_layer(), addresses,
        [self = shared_from_this()](Error ec, const Tcp::endpoint&) {
          if (ec || self->m_stopped) {
            self->close("connect");
          } else {
            self->handshake(ssl::stream_base::client);
          }
        });
  }

  // 握手完成前保持队列关闭；公私两种监听使用各自的 TLS 验证上下文。
  void handshake(ssl::stream_base::handshake_type mode) {
    arm(m_readTimer);
    m_stream.async_handshake(mode, [self = shared_from_this()](Error ec) {
      if (ec || self->m_stopped) {
        self->close("handshake");
        return;
      }

      self->m_readTimer.expires_at(TimePoint::max());
      auto executor = self->m_stream.get_executor();
      self->m_queue->open(
          [weak = self->weak_from_this(), executor](bool overflow) {
            asio::post(executor, [weak, overflow] {
              if (auto session = weak.lock()) {
                if (overflow) {
                  session->close("reliable_queue_full");
                } else {
                  session->writeNext();
                }
              }
            });
          });
      if (self->m_ready) {
        self->m_ready(self->m_queue);
      }
      self->readHeader();
    });
  }

  // 使借出的发送入口失效，再取消未完成的 I/O，最后通知所有者移除连接。
  void close(const char* reason) {
    if (m_stopped) {
      return;
    }
    m_stopped = true;
    m_queue->close();
    m_readTimer.cancel();
    m_writeTimer.cancel();
    Error ignored;
    m_stream.next_layer().close(ignored);
    LOG_INFO("event=tls_close reason={}", reason);
    m_closed(this);
  }

 private:
  // 在单调时钟截止点取消整个会话；检查当前截止点以忽略过期的旧回调。
  void arm(asio::steady_timer& timer) {
    timer.expires_after(m_timeout);
    timer.async_wait([self = shared_from_this(), &timer](Error ec) {
      if (!ec && timer.expiry() <= Clock::now()) {
        self->close("timeout");
      }
    });
  }

  // 空闲长连接可等待首字节；从首字节起到完整帧读毕共用一个固定截止点。
  void readHeader() {
    m_stream.async_read_some(
        asio::buffer(m_header),
        [self = shared_from_this()](Error ec, std::size_t n) {
          if (ec || self->m_stopped) {
            self->close("read");
            return;
          }
          self->arm(self->m_readTimer);
          if (n == self->m_header.size()) {
            self->readBody();
            return;
          }
          auto tail = asio::buffer(self->m_header) + n;
          asio::async_read(self->m_stream, tail, [self](Error ec, std::size_t) {
            if (ec || self->m_stopped) {
              self->close("header");
            } else {
              self->readBody();
            }
          });
        });
  }

  // 确认网络序前缀在协议上限内之后才为载荷分配内存。
  void readBody() {
    std::uint32_t length = 0;
    for (const auto octet : m_header) {
      length = (length << 8) | octet;
    }
    if (length == 0 || length > MAX_REALTIME_FRAME_BYTES) {
      close("frame_length");
      return;
    }
    m_body.resize(length);
    auto received = [self = shared_from_this()](Error ec, std::size_t) {
      if (ec || self->m_stopped) {
        self->close("body");
        return;
      }
      self->m_readTimer.expires_at(TimePoint::max());
      std::weak_ptr<SendQueue> weak = self->m_queue;
      auto reply = [weak](WireBytes payload, std::optional<Id> key) {
        auto queue = weak.lock();
        return queue ? queue->push(std::move(payload), key)
                     : SendStatus::Closed;
      };
      try {
        self->m_receive(std::move(self->m_body), std::move(reply));
      } catch (const std::exception&) {
        self->close("receive");
        return;
      }
      self->readHeader();
    };
    asio::async_read(m_stream, asio::buffer(m_body), std::move(received));
  }

  // 首次入队和上一次写完成时在 I/O 线程调用，同一连接至多有一次在途写。
  void writeNext() {
    auto frame = m_queue->beginWrite();
    if (!frame) {
      return;
    }
    arm(m_writeTimer);
    asio::async_write(
        m_stream, asio::buffer(*frame),
        [self = shared_from_this(), frame](Error ec, std::size_t) {
          if (ec || self->m_stopped) {
            self->close("write");
            return;
          }
          self->m_writeTimer.expires_at(TimePoint::max());
          if (self->m_queue->finishWrite()) {
            self->writeNext();
          }
        });
  }

  ssl::stream<Tcp::socket> m_stream;  // 仅在所属执行器上操作的 TLS 流。
  asio::steady_timer m_readTimer;     // 拨号、握手或收齐一帧的截止点。
  asio::steady_timer m_writeTimer;    // 写完当前整帧的截止点。
  Milliseconds m_timeout;             // 单次操作可用时间。
  std::array<std::uint8_t, STREAM_PREFIX_BYTES> m_header{};  // 当前帧的长度头。
  WireBytes m_body;  // 通过长度校验的载荷。

  std::shared_ptr<SendQueue> m_queue;  // 可由生产者借用的连接专属队列。
  ReceiveFrame m_receive;              // 将完整帧交给逻辑层的回调。
  // 客户端在握手后发布队列，断开时撤回。
  std::function<void(std::shared_ptr<SendQueue>)> m_ready;
  std::function<void(Session*)> m_closed;  // 移除连接或安排重连。
  bool m_stopped = false;                  // 已报告关闭，后续取消回调无副作用。
};

// 按连接数限制握手和活跃会话，已占满时暂停 accept，空出席位后再继续。
class Listener final : public NetworkListener {
 public:
  // 在构造期间完成监听，证书或端口错误直接交给进程入口。
  Listener(Endpoint endpoint, const TlsFiles& files, NetLimits limits,
           ReceiveFrame receive)
      : m_context(MakeContext(files, true)),
        m_acceptor(m_loop.context()),
        m_retry(m_loop.context()),
        m_limits(limits),
        m_receive(std::move(receive)) {
    if (!m_limits.m_maxConnections) {
      throw std::invalid_argument("TLS max_connections must be positive");
    }
    Tcp::resolver resolver(m_loop.context());
    const auto address =
        resolver.resolve(endpoint.m_host, std::to_string(endpoint.m_port))
            .begin()
            ->endpoint();
    m_acceptor.open(address.protocol());
    m_acceptor.set_option(Tcp::acceptor::reuse_address(true));
    m_acceptor.bind(address);
    m_acceptor.listen();
    acceptNext();
    m_loop.start();
  }

  // 在 I/O 线程关闭全部入口，等待取消的读写和握手完成。
  ~Listener() override {
    m_loop.stop([this] {
      m_stopping = true;
      Error ignored;
      m_acceptor.close(ignored);
      m_retry.cancel();
      while (!m_sessions.empty()) {
        auto session = m_sessions.begin()->second;
        session->close("shutdown");
      }
    });
  }

 private:
  // 任意时刻最多等待一次 accept，底层临时失败后延时再接受连接。
  void acceptNext() {
    if (m_stopping || m_accepting ||
        m_sessions.size() >= m_limits.m_maxConnections) {
      return;
    }
    m_accepting = true;
    m_acceptor.async_accept([this](Error ec, Tcp::socket socket) {
      if (m_stopping) {
        return;
      }
      if (ec) {
        m_retry.expires_after(Milliseconds(100));
        m_retry.async_wait([this](Error ec) {
          m_accepting = false;
          if (!ec) {
            acceptNext();
          }
        });
        return;
      }

      m_accepting = false;
      auto session = std::make_shared<Session>(std::move(socket), m_context,
                                               m_limits, m_receive, nullptr,
                                               [this](Session* closed) {
                                                 m_sessions.erase(closed);
                                                 acceptNext();
                                               });
      m_sessions.emplace(session.get(), session);
      session->handshake(ssl::stream_base::server);
      acceptNext();
    });
  }

  IoLoop m_loop;               // 本监听端点的 I/O 执行器。
  ssl::context m_context;      // 本机身份和客户端验证策略。
  Tcp::acceptor m_acceptor;    // 正在监听的 TCP 入口。
  asio::steady_timer m_retry;  // 临时 accept 错误后的重试计时器。
  NetLimits m_limits;          // 同时活跃的连接数和每连接的独立预算。
  ReceiveFrame m_receive;      // 每条连接收到完整帧时交给同一服务入口。
  // 仅由 I/O 线程管理，含尚在握手的连接。
  std::unordered_map<Session*, std::shared_ptr<Session>> m_sessions;
  bool m_accepting = false;  // 已等待 accept 或正在退避。
  bool m_stopping = false;   // 停机后禁止再次接受连接。
};

// 出站 TLS 通道；解析、连接和退避均在专属 I/O 线程上执行。
class Client final : public TlsClient {
 public:
  // 在后台建立第一条连接。send 始终只向已验证身份的会话投递。
  Client(Endpoint endpoint, const TlsFiles& files, NetLimits limits,
         ReceiveFrame receive)
      : m_context(MakeContext(files, false)),
        m_resolver(m_loop.context()),
        m_timer(m_loop.context()),
        m_endpoint(std::move(endpoint)),
        m_peer(files.m_peerName),
        m_limits(limits),
        m_receive(std::move(receive)) {
    resolve();
    m_loop.start();
  }

  // 取消解析、退避和传输，并等待网络回调结束。
  ~Client() override {
    m_loop.stop([this] {
      m_stopping = true;
      m_resolver.cancel();
      m_timer.cancel();
      if (auto active = m_session) {
        active->close("shutdown");
      }
    });
  }

  // 原子取得当前会话独享的队列。重连期间拒绝新帧，旧队列永久关闭。
  SendStatus send(WireBytes payload, std::optional<Id> replace_key) override {
    auto queue = std::atomic_load(&m_outbound);
    return queue ? queue->push(std::move(payload), replace_key,
                                FullQueueAction::Reject)
                 : SendStatus::Closed;
  }

 private:
  // 一次只保持一个解析或连接操作，每次重连都重新解析服务地址。
  void resolve() {
    m_timer.expires_after(m_limits.m_timeout);
    m_timer.async_wait([this](Error ec) {
      if (!ec && m_timer.expiry() <= Clock::now()) {
        m_resolver.cancel();
      }
    });
    m_resolver.async_resolve(
        m_endpoint.m_host, std::to_string(m_endpoint.m_port),
        [this](Error ec, Tcp::resolver::results_type addresses) {
          m_timer.expires_at(TimePoint::max());
          if (m_stopping) {
            return;
          }
          if (ec) {
            retry();
            return;
          }
          auto session = std::make_shared<Session>(
              Tcp::socket(m_loop.context()), m_context, m_limits, m_receive,
              [this](std::shared_ptr<SendQueue> queue) {
                std::atomic_store(&m_outbound, std::move(queue));
                m_backoff = INITIAL_BACKOFF;
              },
              [this](Session*) {
                std::atomic_store(&m_outbound, std::shared_ptr<SendQueue>{});
                m_session.reset();
                retry();
              });
          m_session = session;
          session->connect(std::move(addresses), m_peer);
        });
  }

  // 指数退避带随机抖动，避免多个 Gateway 同时重新拨号。
  void retry() {
    if (m_stopping) {
      return;
    }
    std::uniform_int_distribution<long long> delay(m_backoff.count() / 2,
                                                   m_backoff.count());
    m_timer.expires_after(Milliseconds(delay(m_random)));
    m_backoff = std::min(m_backoff * 2, MAX_BACKOFF);
    m_timer.async_wait([this](Error ec) {
      if (!ec && !m_stopping) {
        resolve();
      }
    });
  }

  // 成功握手后复位的初始重连间隔上限。
  static constexpr Milliseconds INITIAL_BACKOFF{100};
  // 反复失败时单次重连间隔的上限。
  static constexpr Milliseconds MAX_BACKOFF{3000};

  IoLoop m_loop;               // 出站通道的 I/O 执行器。
  ssl::context m_context;      // 本机身份和服务端 CA。
  Tcp::resolver m_resolver;    // 异步服务名解析器。
  asio::steady_timer m_timer;  // 解析截止或下次拨号时刻。
  Endpoint m_endpoint;         // 解析用的目标主机和端口。
  std::string m_peer;          // TLS 校验的服务身份，也是 DNS 连接的 SNI。
  NetLimits m_limits;          // 握手和读写预算。
  ReceiveFrame m_receive;      // 完整回包的处理入口。

  std::shared_ptr<Session> m_session;  // 正在拨号或已经握手的连接。
  // 跨线程发布已握手会话的发送入口，断开时撤回。
  std::shared_ptr<SendQueue> m_outbound;
  Milliseconds m_backoff{INITIAL_BACKOFF};        // 当前重连间隔上限。
  std::mt19937 m_random{std::random_device{}()};  // 重连抖动源。
  bool m_stopping = false;                        // 停机后禁止重拨。
};

}  // namespace

std::unique_ptr<NetworkListener> StartTlsListener(Endpoint endpoint,
                                                  TlsFiles files,
                                                  NetLimits limits,
                                                  ReceiveFrame receive) {
  return std::make_unique<Listener>(std::move(endpoint), files, limits,
                                    std::move(receive));
}

std::unique_ptr<TlsClient> StartTlsClient(Endpoint endpoint, TlsFiles files,
                                          NetLimits limits,
                                          ReceiveFrame receive) {
  if (files.m_peerName.empty()) {
    files.m_peerName = endpoint.m_host;
  }
  return std::make_unique<Client>(std::move(endpoint), files, limits,
                                  std::move(receive));
}

}  // namespace pop
