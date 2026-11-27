// 按内网请求号关联原观察连接，供网关输入回执和普通状态分别核算预算与期限。
#pragma once

#include <condition_variable>
#include <map>
#include <mutex>
#include <optional>
#include <stop_token>
#include <thread>

#include "engine/net/transport.h"

namespace pop {

// 一次内网请求对应的回送位置，原连接断开后发送函数自动失效。
struct ReplyRoute {
  Id m_clientId;     // 当前连接的输入序号或采样查询号。
  SpaceId m_space;   // 转发时从可信配置确定的目标。
  SendFrame m_send;  // 原公网连接的有界发送入口。
};

// 超时后在表锁之外通知原连接；具体消息类型由该通道决定。
using ReplyTimeout = void(const ReplyRoute& route, Id request);

// 每个出站通道独享一张有界在途表。成功和超时争用同一项，只有一方回送。
class PendingReplies {
 public:
  // 使用固定单调时钟等待预算，在最早一项截止时唤醒回送线程。
  PendingReplies(NetLimits limits, ReplyTimeout& on_timeout);

  // 保存原连接并分配通道内唯一的请求号，容量不足时返回零。
  Id reserve(Id client_id, SpaceId space, const SendFrame& send);

  // 以请求号和目标取回路径；输入回执还须核对原序号。已到期的项走超时回送。
  std::optional<ReplyRoute> take(Id request, SpaceId space,
                                 std::optional<Id> expected_client = {});

 private:
  // 从建表时刻计时，序号越小截止越早。
  struct Entry {
    ReplyRoute m_route;    // 等候结果的原观察连接。
    TimePoint m_deadline;  // 本地单调时钟截止点。
  };

  // 将超时通知失败限制在原连接，其他查询继续处理。
  void reportTimeout(const ReplyRoute& route, Id request) const noexcept;

  // 等待最早的截止点，删除已过期项后再向连接投递结果。
  void expire(std::stop_token stop);

  NetLimits m_limits;                     // 本通道的条数与等待预算。
  ReplyTimeout& m_onTimeout;              // 通知该业务的超时结果。
  std::mutex m_mutex;                     // 保护请求号分配及在途表。
  std::condition_variable_any m_changed;  // 最近截止项发生变更时唤醒。
  Id m_next = 0;                          // 本次启动分配的最大内网请求号。
  std::map<Id, Entry> m_entries;          // 同一请求仅移除一次。
  std::jthread m_timer;                   // 析构时先唤醒并结束计时线程。
};

}  // namespace pop
