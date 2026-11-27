// 在 Gateway 汇合发往同一 Space 和 Worker 的连续输入，再交给内网 TLS 队列。
#pragma once

#include <condition_variable>
#include <cstddef>
#include <functional>
#include <list>
#include <mutex>
#include <stop_token>
#include <string>
#include <thread>

#include "engine/net/tls_transport.h"
#include "realtime.pb.h"

namespace pop {

// 有界批处理器；同一绑定仅替换尚未取出的旧输入，已发批次由 Scene 确认。
class InputForwarder {
 public:
  // 按请求号、输入序号和 Space 将未发送的终态交回原连接。
  using Refusal = std::function<void(Id, Id, SpaceId, wire::InputStatus)>;

  // 借用已配置的内网连接，在线程上按首条入队时刻控制批次窗口。
  InputForwarder(TlsClient& client, NetLimits limits, Refusal on_refusal);

  // 结束批处理线程并拒绝已入队但尚未向传输层投递的输入。
  ~InputForwarder();

  // 接管一条可信输入。过期、陈旧、替换和容量不足均通知原请求。
  void post(SpaceId space, Id owner_epoch, const std::string& worker,
            wire::RoutedInput input);

 private:
  // 队列中的单条输入，路由身份随它保存到发送或拒绝时刻。
  struct Item {
    SpaceId m_space;            // 所属 Space。
    Id m_ownerEpoch;            // 该场静态路由的承载代次。
    std::string m_worker;       // 目标 Worker 进程实例标识。
    wire::RoutedInput m_input;  // 从收包时刻起计时的完整输入。
    TimePoint m_deadline;       // 本机单调时钟上的动作截止点。
    std::size_t m_bytes;        // 此项的队列预算占用。
  };

  // 每个窗口按路由分别取件，停止时将留在队列中的输入退还。
  void run(std::stop_token stop);

  // 将同一路由的有效输入编码为一帧；入队成功后等待 Scene 的权威回执。
  void dispatch(std::list<Item>& items);

  // 在锁外回送已知结果，隔离回送端的异常。
  void refuse(Id request, Id sequence, SpaceId space,
              wire::InputStatus status) const;

  TlsClient& m_client;  // 生命周期覆盖批处理线程的内网连接。
  NetLimits m_limits;   // 本转发器的条数、字节预算。
  Refusal m_onRefusal;  // 将结果交回 Gateway 的在途表。

  std::mutex m_mutex;                  // 保护待发送项、窗口和总预算。
  std::condition_variable_any m_wake;  // 新窗口或停机时唤醒发送线程。
  std::list<Item> m_pending;           // 尚可按路由和绑定合并的输入。
  TimePoint m_flushAt{};               // 当前窗口在单调时钟上的截止点。
  std::size_t m_count = 0;             // 含正在成批编码的条数。
  std::size_t m_bytes = 0;             // 含正在成批编码的字节占用。
  std::jthread m_thread;               // 析构时先停止并回收该线程。
};

}  // namespace pop
