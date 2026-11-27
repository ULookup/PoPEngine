// 定义配置和场景运行时共享的值类型，包括时钟、标识、队列预算、实体绑定与快照。

#pragma once

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>

namespace pop {

using Clock = std::chrono::steady_clock;         // 调度与消息有效期的单调时钟。
using TimePoint = Clock::time_point;             // 单调时钟上的一个时刻。
using Milliseconds = std::chrono::milliseconds;  // 以毫秒计的时长。

using SpaceId = std::uint64_t;  // 标识一个地图或战场实例。
using Id = std::uint64_t;       // 实体、请求、代次和序号等使用的整数类型。
using Tick = std::uint64_t;     // 一个 Space 内的模拟步编号。

// 20 Hz 模拟中相邻两步之间的固定时长。
inline constexpr Milliseconds TICK_STEP{50};

// 同一调度轮中，一个 Space 最多补算的步数。
inline constexpr int MAX_CATCH_UP = 2;

// 静态放置的 Space 从首个承载代次开始，由网关与所属 Worker 共同校验。
inline constexpr Id INITIAL_SPACE_OWNER_EPOCH = 1;

// 一个邮箱通道的排队上限和每次消费的来源配额。
struct QueueBudget {
  std::size_t m_entries;    // 同时排队的最大消息数。
  std::size_t m_bytes;      // 同时排队的消息可占用的最大字节数。
  Milliseconds m_age;       // 消息自创建起达到此年龄即过期。
  std::size_t m_perSource;  // 一次取件中同一来源最多可取出的条数。
};

// 一个 Space 的四份通道预算，顺序与 Lane 枚举一致。
using Budgets = std::array<QueueBudget, 4>;

// 指定一次输入针对哪一代实体，以及哪个会话绑定有权控制它。
struct EntityBinding {
  Id m_entity;        // 受控实体的标识。
  Id m_generation;    // 该标识对应的实体生命周期代次。
  Id m_sessionEpoch;  // 当前控制会话的绑定代次。

  // 已分配的实体标识和两个代次都必须为正。
  bool valid() const {
    return m_entity && m_generation && m_sessionEpoch;
  }

  // 三个字段全部相同，两个输入才属于同一绑定。
  bool operator==(const EntityBinding&) const = default;
};

// 本地 Space 的放置配置；实体由所属分片在运行期间绑定。
struct SpaceConfig {
  SpaceId m_id;      // 待创建的 Space 标识。
  unsigned m_shard;  // 推进此 Space 的 Scene 分片下标，从 0 开始。
};

// Scene 分片在每次提交后发布的运行统计，可由其他线程读取副本。
struct Snapshot {
  Tick m_tick = 0;             // 已完成的模拟步编号。
  std::size_t m_entities = 0;  // 此 Space 当前已绑定的实体数。
  Id m_inputs = 0;             // 已处理的匹配绑定及序号的输入数，含到期停止。
  Id m_dispatched = 0;         // 已成功投递到后台队列的任务总数。
  Id m_resultCommits = 0;      // 已通过校验且在分片提交的结果总数。
};

}  // namespace pop
