// 声明样例计数器在 Redis Cluster 中的可重建读视图。
// 接口执行同步基础设施 I/O，调用方在基础设施执行器上运行。

#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>

namespace sw::redis {
class RedisCluster;
}

namespace pop {

struct SampleEvent;
class SampleRepository;

// 连接 Redis Cluster 任一主节点的配置。凭据仅交给客户端。
struct RedisViewConfig {
  std::string m_host = "127.0.0.1";  // 集群种子节点主机。
  int m_port = 6379;                 // 种子节点 TCP 端口。
  std::string m_user = "default";    // Redis ACL 用户名。
  std::string m_password;            // ACL 密码，隔离配置可留空。
  // 建连、读写和等待连接池各自使用的毫秒上限。
  std::chrono::milliseconds m_timeout{2000};
};

// 同一次 Redis 原子更新所安装的聚合水位和计数器绝对值。
struct CounterView {
  std::uint64_t m_version;  // 此聚合已安装的持久版本。
  std::uint64_t m_value;    // 此版本的计数器值。
};

// 一个基础设施客户端可供投影与重建共用。连接或存储错误抛给调用方重试。
class SampleReadView {
 public:
  // 从种子节点发现槽位，配置有界的连接和等待时间。
  explicit SampleReadView(const RedisViewConfig& config);
  // 释放客户端持有的连接。
  ~SampleReadView();

  // 按聚合版本原子安装绝对值。相同或旧版本返回 false。
  bool install(const SampleEvent& event);
  // 返回同一版本的水位与值。键尚未安装时返回空值。
  std::optional<CounterView> read(std::uint64_t aggregate_id);

  // 从持久来源读取并安装一个聚合的快照，返回随快照取得的水位。
  // 尚无该聚合的记录时返回 0。此水位供同一聚合继续重建。
  std::uint64_t beginRebuild(SampleRepository& repository,
                             std::uint64_t aggregate_id);
  // 读取本次追赶的持久高水位，分页补齐快照之后的事件并返回高水位。
  // 更晚的提交沿消息订阅投影。失败可用原快照水位重试。
  std::uint64_t finishRebuild(SampleRepository& repository,
                              std::uint64_t aggregate_id,
                              std::uint64_t snapshot_version);

 private:
  // 槽位发现及每个节点的连接池，由 redis-plus-plus 维护。
  std::unique_ptr<sw::redis::RedisCluster> m_cluster;
};

}  // namespace pop
