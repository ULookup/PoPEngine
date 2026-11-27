// 在 Redis 主节点按源版本安装绝对计数，按聚合独立的持久水位追赶重建期间的提交。

#include "engine/cache/sample_read_view.h"

#include <sw/redis++/redis++.h>

#include <charconv>
#include <cstddef>
#include <iterator>
#include <stdexcept>
#include <system_error>
#include <vector>

#include "storage/sample_repository.h"

namespace pop {
namespace {

// 每批仅保留有限条持久事件，避免大聚合重建时占用无界内存。
constexpr std::size_t REPLAY_PAGE_SIZE = 128;

// 版本用规范十进制串比较，避免 Lua number 超过 2^53 后丢失精度。
// 一个命令同时安装水位与值，重建快照和订阅投影可以并发执行。
constexpr char INSTALL_SCRIPT[] = R"lua(
local old = redis.call('HGET', KEYS[1], 'version')
local next_version = ARGV[1]
if old and (#old > #next_version or
            (#old == #next_version and old >= next_version)) then
  return 0
end
redis.call('HSET', KEYS[1], 'version', next_version, 'value', ARGV[2])
return 1
)lua";

// 将聚合标识放入 Redis 哈希标签，同一聚合始终路由到同一槽。
std::string ViewKey(std::uint64_t aggregate_id) {
  return "pop:phase3:counter:{" + std::to_string(aggregate_id) + "}";
}

// 校验缓存中的十进制数，损坏的值由调用方触发读视图恢复。
std::uint64_t ParseCounterField(const std::string& field) {
  std::uint64_t value = 0;
  const auto* end = field.data() + field.size();
  const auto parsed = std::from_chars(field.data(), end, value);
  if (parsed.ec != std::errc() || parsed.ptr != end) {
    throw std::runtime_error("invalid cached counter field");
  }
  return value;
}

}  // namespace

SampleReadView::SampleReadView(const RedisViewConfig& config) {
  if (config.m_host.empty() || config.m_port < 1 || config.m_port > 65535 ||
      config.m_timeout.count() <= 0) {
    throw std::invalid_argument("invalid Redis Cluster endpoint or timeout");
  }
  sw::redis::ConnectionOptions connection;
  connection.host = config.m_host;
  connection.port = config.m_port;
  connection.user = config.m_user;
  connection.password = config.m_password;
  connection.connect_timeout = config.m_timeout;
  connection.socket_timeout = config.m_timeout;
  sw::redis::ConnectionPoolOptions pool;
  pool.wait_timeout = config.m_timeout;
  m_cluster = std::make_unique<sw::redis::RedisCluster>(connection, pool);
}

SampleReadView::~SampleReadView() = default;

bool SampleReadView::install(const SampleEvent& event) {
  if (event.m_version == 0) {
    throw std::invalid_argument("counter event has no committed version");
  }
  const auto key = ViewKey(event.m_aggregateId);
  const auto version = std::to_string(event.m_version);
  const auto value = std::to_string(event.m_value);
  const auto installed =
      m_cluster->eval<long long>(INSTALL_SCRIPT, {key}, {version, value});
  return installed == 1;
}

std::optional<CounterView> SampleReadView::read(std::uint64_t aggregate_id) {
  std::vector<sw::redis::OptionalString> fields;
  m_cluster->hmget(ViewKey(aggregate_id), {"version", "value"},
                   std::back_inserter(fields));
  if (fields.size() == 2 && !fields[0] && !fields[1]) return std::nullopt;
  if (fields.size() != 2 || !fields[0] || !fields[1]) {
    throw std::runtime_error("incomplete cached counter");
  }
  return CounterView{
      .m_version = ParseCounterField(*fields[0]),
      .m_value = ParseCounterField(*fields[1]),
  };
}

std::uint64_t SampleReadView::beginRebuild(SampleRepository& repository,
                                           std::uint64_t aggregate_id) {
  auto snapshot = repository.snapshot(aggregate_id);
  if (!snapshot) return 0;
  install(*snapshot);
  return snapshot->m_version;
}

std::uint64_t SampleReadView::finishRebuild(SampleRepository& repository,
                                            std::uint64_t aggregate_id,
                                            std::uint64_t snapshot_version) {
  const auto latest = repository.snapshot(aggregate_id);
  const auto high_water = latest ? latest->m_version : 0;
  if (snapshot_version > high_water) {
    throw std::invalid_argument("snapshot exceeds durable aggregate version");
  }
  auto cursor = snapshot_version;
  while (cursor < high_water) {
    const auto page = repository.eventsAfter(aggregate_id, cursor, high_water,
                                             REPLAY_PAGE_SIZE);
    if (page.empty()) throw std::runtime_error("missing durable counter event");
    for (const auto& event : page) {
      if (event.m_version != cursor + 1 ||
          event.m_aggregateId != aggregate_id || event.m_version > high_water) {
        throw std::runtime_error("counter replay has a version gap");
      }
      install(event);
      cursor = event.m_version;
    }
  }
  // 重建任务可能在快照安装后被中断，Redis 之后也可能丢失该键。
  // 即使没有后续事件，重试完成时也须有已安装的高水位或更新版本。
  if (latest) install(*latest);
  return high_water;
}

}  // namespace pop
