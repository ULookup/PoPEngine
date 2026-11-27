// 在独立的 Asio 执行线程上处理 Nginx 转发的健康查询，每个请求完成后关闭连接。
#include <boost/asio.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <chrono>
#include <functional>
#include <memory>
#include <stdexcept>
#include <thread>
#include <utility>

#include "engine/net/control_endpoints.h"

namespace pop {
namespace {

namespace asio = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
using Tcp = asio::ip::tcp;

// 健康端点只接受请求头，此预算含路径和所有 HTTP 字段。
constexpr std::size_t HEADER_BYTES = 8192;

// 一次 HTTP 请求持有自己的解析器、发送内容和读写截止点。
class HttpSession : public std::enable_shared_from_this<HttpSession> {
 public:
  // 接管新连接，结束时由 done 归还监听器的一份并发配额。
  HttpSession(Tcp::socket socket, Milliseconds timeout,
              std::function<void()> done)
      : m_stream(std::move(socket)),
        m_timeout(timeout),
        m_done(std::move(done)) {
    m_request.header_limit(HEADER_BYTES);
    m_request.body_limit(0);
  }

  // 读到完整请求后处理一次，在发生错误或超时时立即结束。
  void start() {
    m_stream.expires_after(m_timeout);
    http::async_read(
        m_stream, m_buffer, m_request,
        [self = shared_from_this()](beast::error_code error, std::size_t) {
          if (error) {
            self->finish();
          } else {
            self->respond();
          }
        });
  }

 private:
  // 返回端点状态，其他路径和方法分别返回 404 和 405。
  void respond() {
    const auto& request = m_request.get();
    m_response.version(request.version());
    m_response.keep_alive(false);
    if (request.target() != "/healthz") {
      m_response.result(http::status::not_found);
    } else if (request.method() != http::verb::get) {
      m_response.result(http::status::method_not_allowed);
      m_response.set(http::field::allow, "GET");
    } else {
      m_response.result(http::status::ok);
      m_response.set(http::field::content_type, "application/json");
      m_response.body() = R"({"status":"serving","role":"platform"})";
    }
    m_response.prepare_payload();

    m_stream.expires_after(m_timeout);
    http::async_write(m_stream, m_response,
                      [self = shared_from_this()](
                          beast::error_code, std::size_t) { self->finish(); });
  }

  // 在同一执行线程关闭套接字并通知监听器继续接收连接。
  void finish() {
    beast::error_code ignored;
    m_stream.socket().shutdown(Tcp::socket::shutdown_both, ignored);
    m_stream.socket().close(ignored);
    m_done();
  }

  beast::tcp_stream m_stream;    // 持有套接字与截止计时器。
  Milliseconds m_timeout;        // 单次请求读取或回复写入的最长时间。
  std::function<void()> m_done;  // 释放一个活动连接名额。
  beast::flat_buffer m_buffer{HEADER_BYTES};  // HTTP 解析的有界读取缓冲。
  http::request_parser<http::empty_body> m_request;  // 请求解析状态。
  http::response<http::string_body> m_response;      // 异步写入期间持有回复。
};

// 单线程串行受理 HTTP 连接。连接达到预算后暂停 accept，直到有连接结束。
class PlatformHealth final : public NetworkListener {
 public:
  // 绑定内网地址并启动事件循环，监听失败时向进程入口报告异常。
  PlatformHealth(Endpoint endpoint, NetLimits limits)
      : m_limits(limits), m_acceptor(m_io), m_retry(m_io) {
    if (!m_limits.m_maxConnections || m_limits.m_timeout.count() <= 0) {
      throw std::invalid_argument("invalid Platform HTTP limits");
    }
    Tcp::endpoint address(asio::ip::make_address(endpoint.m_host),
                          endpoint.m_port);
    m_acceptor.open(address.protocol());
    m_acceptor.set_option(Tcp::acceptor::reuse_address(true));
    m_acceptor.bind(address);
    m_acceptor.listen();
    accept();
    m_thread = std::thread([this] { m_io.run(); });
  }

  // 停止事件循环并等候线程，随后释放监听器和所有未完成的会话。
  ~PlatformHealth() override {
    m_io.stop();
    m_thread.join();
  }

 private:
  // 在配额允许时仅发起一次 accept，临时系统错误后稍作退避。
  void accept() {
    if (m_acceptPending || m_active == m_limits.m_maxConnections) {
      return;
    }
    m_acceptPending = true;
    m_acceptor.async_accept(
        [this](beast::error_code error, Tcp::socket socket) {
          if (error) {
            m_retry.expires_after(std::chrono::milliseconds(100));
            m_retry.async_wait([this](beast::error_code retry_error) {
              m_acceptPending = false;
              if (!retry_error) {
                accept();
              }
            });
            return;
          }

          m_acceptPending = false;
          ++m_active;
          std::make_shared<HttpSession>(std::move(socket), m_limits.m_timeout,
                                        [this] {
                                          --m_active;
                                          accept();
                                        })
              ->start();
          accept();
        });
  }

  NetLimits m_limits;            // 健康端点使用的连接数和时限。
  asio::io_context m_io{1};      // 所有会话共用的串行执行线程。
  Tcp::acceptor m_acceptor;      // Platform 内网 HTTP 监听器。
  asio::steady_timer m_retry;    // 暂时无法接入连接时的重试计时器。
  std::size_t m_active = 0;      // 当前正在处理的连接数。
  bool m_acceptPending = false;  // 是否已在等待连接或重试。
  std::thread m_thread;          // 进程拥有的 HTTP 执行线程。
};

}  // namespace

std::unique_ptr<NetworkListener> StartPlatformHealth(Endpoint endpoint,
                                                     NetLimits limits) {
  return std::make_unique<PlatformHealth>(std::move(endpoint), limits);
}

}  // namespace pop
