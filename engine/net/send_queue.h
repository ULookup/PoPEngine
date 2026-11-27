// 管理一个 TCP/TLS 会话的有界发送队列。生产者在入队时占用预算，
// I/O 线程串行取帧，在写完成后归还预算。
#pragma once

#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>

#include "engine/net/transport.h"

namespace pop {

// 可靠帧无法排入时，回执通道可以断开，内部批次的生产者可直接回复背压。
enum class FullQueueAction {
  Close,  // 结束连接，让接收方感知必达消息未能送出。
  Reject  // 仅拒绝当前批次，先前已排队的帧继续发送。
};

// TLS 写队列，正在写的一帧也占用条数与字节预算。
class SendQueue {
 public:
  // 使用 limits 中的单连接条数与字节上限。
  explicit SendQueue(NetLimits limits);

  // 握手成功后启用队列；schedule 仅向 I/O 执行器投递，不重入队列。
  // true 请求断开满载的可靠通道，false 请求开始写。
  void open(std::function<void(bool)> schedule);

  // 同键待写状态可合并。可靠帧超预算时通知断线，调用方获知背压。
  SendStatus push(WireBytes payload, std::optional<std::uint64_t> replace_key,
                  FullQueueAction on_full = FullQueueAction::Close);

  // 锁定队首并借出独立持有的完整 TCP 帧，空队列或已有在途写时返回空。
  std::shared_ptr<const WireBytes> beginWrite();

  // I/O 完成后归还队首的预算，返回是否还有帧待写。
  bool finishWrite();

  // 关闭后拒绝投递，丢弃本连接尚未写出的数据。
  void close();

 private:
  // 一项待写消息；可替换项仅在进入套接字前参与合并。
  struct Item {
    std::optional<std::uint64_t> m_key;       // 会话内替换键，空值表示可靠。
    std::shared_ptr<const WireBytes> m_wire;  // 含长度前缀的完整数据。
  };

  const NetLimits m_limits;  // 每条连接独享的预算。
  std::mutex m_mutex;        // 串行化跨线程入队和 I/O 完成。
  std::deque<Item> m_items;  // 包括在途写的全部帧。
  std::size_t m_bytes = 0;   // 所有帧的线上字节数。
  bool m_writing = false;    // 队首已交给套接字，禁止原位替换。

  // 发给本连接 I/O 线程的唤醒函数，空值表示当前不受理。
  std::function<void(bool)> m_schedule;
};

}  // namespace pop
