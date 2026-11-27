// 声明样例分片的事务入口；每个调用借用一条 Router 主写连接，返回前释放。

#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "engine/core/types.h"

namespace odb::mysql {
class database;
}

namespace pop {

// 一个逻辑数据分片的 Router 主写端点与连接预算。
struct SampleDatabaseConfig {
  std::string m_user;             // 隔离库的访问账号。
  std::string m_password;         // 访问凭据，仅在建连时使用。
  std::string m_database;         // 存放样例表的库名。
  std::string m_host;             // Router 主写入口主机。
  unsigned m_port = 0;            // Router TCP 端口。
  std::size_t m_connections = 4;  // 同时借出的连接数上限。
};

// 一次正向计数命令；重试必须复用原操作号和全部参数。
struct SampleCommand {
  std::uint64_t m_operationId;  // 提交前由调用方保存的稳定标识。
  std::uint64_t m_aggregateId;  // 同一分片内的计数器标识。
  std::uint64_t m_delta;        // 本次要增加的正整数。
};

// 样例每次操作发布一个携带已提交绝对值的事件。
struct SampleEvent {
  std::uint64_t m_eventId = 0;        // 此样例一笔操作恰好一个事件。
  std::uint64_t m_operationId = 0;    // 首次提交使用的稳定业务标识。
  std::uint64_t m_aggregateId = 0;    // 计数器标识。
  std::uint64_t m_version = 0;        // 聚合内从 1 开始的提交序号。
  std::uint64_t m_value = 0;          // 该版本的绝对计数。
  std::uint64_t m_committedAtMs = 0;  // 事务写入事件时的数据库 Unix 毫秒。
};

// 命令受理、权威查询与提交结果各自的完成状态。
enum class SampleStatus {
  Committed,    // 查到或新提交了原操作的结果。
  Missing,      // 本次权威查询尚未发现已提交操作。
  Rejected,     // 截止、参数冲突、非法输入或计数超出范围。
  Unknown,      // 已发起提交但结果未知，沿原标识查询。
  Unavailable,  // 本次访问未进入提交或未能读到权威结果，可重试。
};

// 值类型结果，只有 Committed 携带已提交事件。
struct SampleResult {
  SampleStatus m_status;  // 当前调用的结论。
  SampleEvent m_event;    // 可供调用方按版本安装的持久状态。
};

// 一次 Outbox 认领，由数据库分配单行递增代次，过期后可重新认领。
struct OutboxClaim {
  SampleEvent m_event;         // 待发布的持久事件。
  std::uint64_t m_generation;  // 此次认领的隔离标识。
  std::uint64_t m_attempt;     // 从上一次人工重放开始计数的尝试次数。
};

// 样例持久化入口，阻塞操作由数据库线程或专用基础设施线程调用。
class SampleRepository {
 public:
  // 绑定单个逻辑分片；池的并发与数据库账号均由调用方配置。
  explicit SampleRepository(const SampleDatabaseConfig& config);
  // 由持有方在所有访问线程退出后释放分片连接池。
  ~SampleRepository();

  // 在同一事务保存累计值、操作结果与 Outbox；截止后不再发起提交。
  SampleResult apply(SampleCommand command,
                     TimePoint deadline = TimePoint::max());
  // 从主写入口按原操作号查询；未读到结果时可继续查询或重提原操作。
  SampleResult query(std::uint64_t operation_id);

  // 在短事务中锁定一个到期事件，租期由数据库时钟计算。
  std::optional<OutboxClaim> claimOutbox(Milliseconds lease);
  // 仅在本次认领仍有效时标记成功，返回写入是否生效。
  bool markPublished(const OutboxClaim& claim);
  // 失败后延迟再次认领，或将事件保留为隔离未完成状态。
  bool deferOutbox(const OutboxClaim& claim, Milliseconds delay,
                   bool quarantine);
  // 按原事件标识重新开放隔离事件，返回是否生效。
  bool replayOutbox(std::uint64_t event_id);

  // 同一事务提交去重记录和样例投影，重复返回 false，驱动失败抛异常。
  bool applyInbox(const SampleEvent& event);
  // 读取消费端实际提交的不同事件数，无记录时为零。
  std::uint64_t projectionApplications(std::uint64_t aggregate_id);

  // 从持久事件取得最新计数和同一版本的重建水位。
  std::optional<SampleEvent> snapshot(std::uint64_t aggregate_id);
  // 按版本升序读取 (after_version, through_version] 内至多 limit 项。
  std::vector<SampleEvent> eventsAfter(std::uint64_t aggregate_id,
                                       std::uint64_t after_version,
                                       std::uint64_t through_version,
                                       std::size_t limit);

 private:
  // 本分片的 ODB 主写连接池，事务上下文在调用线程栈上。
  std::unique_ptr<odb::mysql::database> m_database;
};

}  // namespace pop
