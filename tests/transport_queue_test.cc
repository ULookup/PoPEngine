// 验证一条慢 TLS 连接合并待发位姿并向可靠生产者背压，另一连接可继续写。
#include <array>
#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <chrono>
#include <cstdint>
#include <exception>
#include <future>
#include <iostream>
#include <stdexcept>
#include <string>
#include <utility>

#include "engine/net/send_queue.h"
#include "engine/net/tls_transport.h"

namespace pop {
namespace {

namespace asio = boost::asio;
namespace ssl = asio::ssl;
using Tcp = asio::ip::tcp;
using Stream = ssl::stream<Tcp::socket>;

// 失败时报告具体的队列操作，便于定位预算或写顺序错误。
void Check(bool ok, const char* reason) {
  if (!ok) {
    throw std::runtime_error(reason);
  }
}

// 由测试显式完成在途写，确定性地控制慢连接何时释放预算。
void SlowWriter() {
  NetLimits limits{};
  limits.m_queueEntries = 3;
  limits.m_queueBytes = 18;
  SendQueue slow(limits);
  unsigned wakes = 0;
  bool disconnected = false;
  slow.open([&](bool close) {
    if (close) {
      disconnected = true;
    } else {
      ++wakes;
    }
  });

  Check(slow.push({1}, 17) == SendStatus::Queued, "first pose not queued");
  const auto in_flight = slow.beginWrite();
  Check(in_flight && *in_flight == WireBytes{0, 0, 0, 1, 1},
        "incorrect TCP prefix or payload");

  Check(slow.push({2}, 17) == SendStatus::Queued,
        "new pose overwrote the in-flight frame");
  Check(slow.push({3}, {}) == SendStatus::Queued, "reliable event not queued");
  Check(slow.push({4, 5, 6, 7, 8}, 17) == SendStatus::Full,
        "replacement bypassed byte budget");
  Check(slow.push({4, 5, 6, 7}, 17) == SendStatus::Merged,
        "replacement at exact byte budget failed");
  Check(wakes == 1 && !disconnected && !slow.beginWrite(),
        "extra writer or disconnect while merging");
  Check(in_flight->back() == 1 && slow.finishWrite(),
        "in-flight frame changed before completion");

  const auto event = slow.beginWrite();
  Check(event && event->back() == 3 && slow.finishWrite(),
        "new pose overtook an older reliable event");
  const auto latest = slow.beginWrite();
  Check(latest && *latest == WireBytes{0, 0, 0, 4, 4, 5, 6, 7},
        "coalescing delivered an obsolete pose");

  const auto rejected = slow.push(WireBytes(7), {}, FullQueueAction::Reject);
  Check(rejected == SendStatus::Full && !disconnected,
        "refusing an internal batch disconnected the active channel");
  Check(slow.finishWrite() == false && slow.push({8}, {}) == SendStatus::Queued,
        "refusing a batch prevented further traffic after drain");

  const auto full = slow.push(WireBytes(11), {});
  Check(full == SendStatus::Full && disconnected,
        "reliable overflow did not signal disconnect");
  Check(slow.push({9}, {}) == SendStatus::Closed,
        "old session accepted more reliable messages");
}

// 发送或读取一个字节的测试载荷，收包时连同线上长度头一起核对。
void Ping(Stream& stream, std::uint8_t value) {
  const std::array<std::uint8_t, 5> expected{0, 0, 0, 1, value};
  asio::write(stream, asio::buffer(expected));
  std::array<std::uint8_t, 5> result{};
  asio::read(stream, asio::buffer(result));
  Check(result == expected, "TLS echo delivered the wrong frame");
}

// 同时建立两个真实客户端。慢连接生产者在同一轮 I/O 中灌满队列，
// 避免依赖内核缓冲区大小或人为睡眠来触发背压。
void SlowConnection(Endpoint endpoint, TlsFiles server, const TlsFiles& peer) {
  NetLimits limits{};
  limits.m_queueEntries = 2;
  limits.m_queueBytes = 10;
  std::promise<std::array<SendStatus, 4>> observed;
  auto statuses = observed.get_future();
  auto listener =
      StartTlsListener(endpoint, std::move(server), limits,
                       [&](WireBytes request, SendFrame reply) {
                         if (request != WireBytes{0}) {
                           reply(std::move(request), {});
                           return;
                         }
                         observed.set_value({reply({1}, 17), reply({2}, 17),
                                             reply({3}, {}), reply({4}, {})});
                       });

  asio::io_context io;
  ssl::context context(ssl::context::tls_client);
  context.load_verify_file(peer.m_ca);
  context.set_verify_mode(ssl::verify_peer);
  context.set_verify_callback(ssl::host_name_verification(peer.m_peerName));

  const Tcp::endpoint address(asio::ip::make_address(endpoint.m_host),
                              endpoint.m_port);
  Stream healthy(io, context);
  healthy.next_layer().connect(address);
  healthy.handshake(ssl::stream_base::client);
  Ping(healthy, 8);

  Stream slow(io, context);
  slow.next_layer().connect(address);
  slow.handshake(ssl::stream_base::client);
  const std::array<std::uint8_t, 5> trigger{0, 0, 0, 1, 0};
  asio::write(slow, asio::buffer(trigger));
  Check(statuses.wait_for(std::chrono::seconds(2)) == std::future_status::ready,
        "slow TLS producer did not report backpressure");
  Check(statuses.get() == std::array{SendStatus::Queued, SendStatus::Merged,
                                     SendStatus::Queued, SendStatus::Full},
        "wrong TCP admission on a congested connection");

  boost::system::error_code closed;
  std::array<std::uint8_t, 1> byte{};
  slow.read_some(asio::buffer(byte), closed);
  Check(static_cast<bool>(closed),
        "overflowed reliable connection stayed open");
  Ping(healthy, 9);
}

// 真实服务端依次接受和关闭两代连接，分别主动送入一帧来触发客户端回送。
// 第二次握手与应答证明出站链路能在对端重启后恢复。
void Reconnect(const TlsFiles& server, const TlsFiles& peer) {
  asio::io_context io;
  ssl::context context(ssl::context::tls_server);
  context.use_certificate_chain_file(server.m_certificate);
  context.use_private_key_file(server.m_key, ssl::context::pem);
  Tcp::acceptor acceptor(io, {asio::ip::make_address("127.0.0.1"), 0});
  auto client =
      StartTlsClient({"127.0.0.1", acceptor.local_endpoint().port()}, peer,
                     NetLimits{}, [](WireBytes payload, SendFrame reply) {
                       reply(std::move(payload), {});
                     });
  for (std::uint8_t attempt : {1, 2}) {
    Stream incoming(acceptor.accept(), context);
    incoming.handshake(ssl::stream_base::server);
    Ping(incoming, attempt);
  }
}

}  // namespace
}  // namespace pop

// 从 CTest 运行发送队列的独立回归用例。
int main(int argc, char** argv) {
  try {
    if (argc == 1) {
      pop::SlowWriter();
    } else if (argc == 6) {
      const auto port = static_cast<std::uint16_t>(std::stoul(argv[1]));
      const pop::TlsFiles server{"", argv[2], argv[3], ""};
      const pop::TlsFiles peer{argv[4], "", "", argv[5]};
      pop::SlowConnection({"127.0.0.1", port}, server, peer);
      pop::Reconnect(server, peer);
    } else {
      throw std::runtime_error("expected port cert key ca peer_name");
    }
    return 0;
  } catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return 1;
  }
}
