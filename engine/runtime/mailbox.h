// 声明每个 Space 的有界邮箱，以及控制请求、连续输入、计算任务和结果的消息类型。
// 投递方通过返回值区分受理、合并和拒绝。

#pragma once

#include <array>
#include <cstddef>
#include <deque>
#include <functional>
#include <future>
#include <mutex>
#include <span>
#include <string>
#include <variant>
#include <vector>

#include "engine/core/types.h"

namespace pop {

// 每个 Space 的消息通道。枚举顺序也是队列和预算数组的下标。
enum class Lane {
  Control,    // 由 Scene 分片处理、需要返回终态的控制请求。
  Input,      // 由 Scene 分片在 Tick 中采纳的连续控制输入。
  Result,     // 计算线程回投给 Scene 分片、等待校验的结果。
  Background  // Scene 分片派发给计算线程的只读任务。
};

// 一次投递的即时结果。消息入队后仍要等待消费者处理。
enum class Admission {
  Accepted,     // 新增了一条待处理消息。
  Merged,       // 新输入已替换队列中同一绑定的旧输入。
  Full,         // 入队或替换会超过本通道的条数或字节上限。
  Stale,        // 输入序号小于或等于同一绑定已排队的序号。
  Expired,      // 投递时已达到队列年龄上限或输入有效期。
  Invalid,      // 创建时间在未来，或绑定、输入序号、计算函数无效。
  Closed,       // 邮箱已关闭投递入口。
  UnknownSpace  // 路由时找不到目标 Space，消息尚未到达邮箱。
};

// 返回投递结果在日志中使用的名称，例如 "accepted" 或 "full"。
const char* Name(Admission admission);

// 由场景模块解释的进程内数据，自持有内存，可随消息跨线程转移。
using Bytes = std::vector<std::byte>;

// 同一绑定的较新输入可替换旧输入，载荷包含完整的连续控制状态。
struct ControlInput {
  EntityBinding m_binding;  // 输入对应的实体、生命周期代次与会话代次。
  Id m_seq;                 // 当前绑定内递增的输入序号，从正数开始。
  Bytes m_state;            // 由生产者编码、场景模块解释的完整状态。
  TimePoint m_validUntil;   // 单调时钟截止点，到达此刻就停止沿用输入。
};

// 连续输入在 Scene 提交或队列中终止的原因。
enum class InputResult {
  Applied,     // 本步采纳并已发布快照。
  Expired,     // 本步处理了已过期输入。
  Stale,       // 绑定或序号已经失效。
  Superseded,  // 在队列中已被较新输入覆盖。
  Cancelled    // 停机清理了仍在排队的输入。
};

// 仅做有界非阻塞投递。Tick 为零表示消息在 Scene 采样前即已结束。
using InputReply = std::function<void(InputResult, Tick)>;

// 在锁外通知输入终态。调用方的通知失败不会中断已经完成的入队或提交。
void NotifyInput(const InputReply& reply, InputResult result,
                 Tick tick) noexcept;

// 已受理的控制请求最终向发起方报告的处理结果。
enum class RequestStatus {
  Completed,     // Scene 分片已在 Tick 中处理请求。
  Expired,       // 请求被取出时已达到队列年龄上限。
  StaleBinding,  // 请求的实体或会话代次已被更新。
  Cancelled      // 关闭期间，仍在排队的请求被取消。
};

// 要求 Scene 分片确认处理状态的控制请求，每个请求有独立回执。
struct Inspect {
  Id m_requestId;                       // 关联请求和处理日志的标识。
  std::promise<RequestStatus> m_reply;  // 向发起方的 future 写入终态。
};

// 可信的本地控制请求，由 Scene 分片安装绑定并向发起方确认终态。
struct BindEntity {
  EntityBinding m_binding;              // 要安装的实体和控制会话代次。
  std::promise<RequestStatus> m_reply;  // 返回完成、陈旧、过期或取消。
};

// 计算函数只接收任务自有的只读快照，返回独立的结果数据。
using ComputeFunction = Bytes (*)(std::span<const std::byte> snapshot);

// 标识一次快照计算的来源，用于阻止旧结果写入当前场景或新实体。
struct WorkKey {
  SpaceId m_space;  // 生成快照的 Space。
  Id m_ownerEpoch;  // 派发时该 Space 的承载代次。
  Tick m_tick;      // 快照所属的服务器模拟步。
  Id m_stage;       // 派发任务的模拟阶段。
  Id m_version;     // 计算所依据的实体状态版本。
  Id m_entity;      // 参与计算的实体标识。
  Id m_generation;  // 派发时该实体的生命周期代次。

  // 逐字段比较任务身份，提交方据此识别待提交的那次计算。
  bool operator==(const WorkKey&) const = default;
};

// 发给计算线程的只读任务，包含完成计算所需的数据副本。
struct Work {
  WorkKey m_key;              // 任务身份，将原样带回结果。
  Bytes m_snapshot;           // 派发时复制或移交的只读阶段数据。
  ComputeFunction m_compute;  // 计算线程在快照上执行的函数。
};

// 计算线程回投的候选结果，由所属 Scene 分片决定能否提交。
struct WorkResult {
  WorkKey m_key;   // 被计算的原任务身份。
  Bytes m_output;  // 等待场景分片提交的计算输出。
};

// 在快照上执行任务指定的计算，并保留全部原任务身份。
WorkResult Compute(const Work& work);

// 消息的实际类型决定入队通道；绑定与查询共用控制预算。
using Payload =
    std::variant<Inspect, BindEntity, ControlInput, WorkResult, Work>;

// 跨线程投递的一条值消息；目标 Space 由投递时选中的邮箱确定。
struct Message {
  Id m_source;          // 来源标识，同一通道按此标识分配取件配额。
  TimePoint m_created;  // 单调时钟上的创建时刻，用于计算年龄和筛选批次。
  Payload m_payload;    // 消费者要处理的请求、输入、结果或任务。
  std::string m_trace;  // 可选跟踪标识，用于关联投递与处理日志。
  InputReply m_inputReply{};  // 可选输入回执，不随持续控制状态保存。

  // 返回计入队列预算的字节数，包括消息对象以及跟踪串和载荷的容量。
  std::size_t bytes() const;
};

// 从邮箱取出的一项，消费者根据过期标志选择完成、停止输入或丢弃。
struct Delivery {
  Message m_message;  // 已从队列移交给消费者的消息。
  bool m_expired;     // 在本次取件的 now 时刻已达到年龄上限或输入截止点。
};

// 单个通道的观测值，包括当前占用和从创建起累计的事件次数。
struct QueueStats {
  std::size_t m_entries = 0;   // 当前排队条数。
  std::size_t m_bytes = 0;     // 当前排队消息占用的预算字节数。
  Milliseconds m_oldestAge{};  // 当前最老消息的年龄，空队列为零。
  Id m_merged = 0;             // 较新输入成功覆盖待处理输入的次数。
  Id m_rejected = 0;           // 邮箱拒绝投递的总次数。
  Id m_expired = 0;            // 出队时被标为过期的消息总数。

  // 按 Admission 原因累计的投递拒绝次数。
  std::array<Id, static_cast<std::size_t>(Admission::UnknownSpace) + 1>
      m_rejections{};

  // 查询给定原因的拒绝次数，例如队列满或输入序号陈旧。
  Id rejectedFor(Admission reason) const {
    return m_rejections[static_cast<std::size_t>(reason)];
  }
};

// 一个 Space 的线程间消息队列。生产者按 Space 投递，Scene 分片和
// 计算线程各自消费对应通道。游戏内的玩家邮件由 MailService 负责。
class Mailbox {
 public:
  // 用四个通道各自的预算创建一个可以立即接收消息的空邮箱。
  explicit Mailbox(Budgets budgets);

  // 按消息内容选择通道，以 now 检查有效期，返回当次入队或合并结果。
  Admission post(Message message, TimePoint now);

  // 取出创建时间不晚于 cutoff 的消息，每个来源最多取其配额条数。
  // 过期项也随批次返回，以 now 判定是否过期。
  std::vector<Delivery> take(Lane lane, TimePoint now, TimePoint cutoff);

  // 返回 lane 在 now 时刻的占用、最老消息年龄与累计计数的副本。
  QueueStats stats(Lane lane, TimePoint now) const;

  // 关闭后续投递，已排队消息仍可取出或取消。
  void close();

  // 清空所有通道，向待处理控制请求回复 Cancelled，返回清除的总条数。
  std::size_t cancelRemaining();

 private:
  // 单个通道的排队消息与计数，由邮箱的互斥锁保护。
  struct Queue {
    std::deque<Message> m_items;  // 待取消息按入队顺序排列，合并时原位替换。
    QueueStats m_stats;           // 预算占用字节与事件计数，随入队和出队更新。
  };

  Budgets m_budgets;              // 各通道的容量、最大年龄和来源配额。
  mutable std::mutex m_mutex;     // 保护队列、计数和投递入口的关闭状态。
  std::array<Queue, 4> m_queues;  // 各通道独立的队列，按 Lane 下标访问。
  bool m_closed = false;          // 为真时，后续 post 返回 Closed。
};

}  // namespace pop
