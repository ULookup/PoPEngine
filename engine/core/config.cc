// 解析服务端进程配置，在启动前校验角色、队列预算和 Space 放置。

#include "engine/core/config.h"

#include <yaml-cpp/yaml.h>

#include <limits>
#include <set>
#include <stdexcept>

namespace pop {
namespace {

// 从 YAML 读取整数预算或下标，校验调用方指定的下限与 int 上限。
unsigned Number(const YAML::Node& node, const std::string& key,
                int minimum = 1) {
  auto value = node[key].as<long long>(-1);
  if (value < minimum || value > std::numeric_limits<int>::max()) {
    throw std::runtime_error("invalid or missing " + key);
  }
  return static_cast<unsigned>(value);
}

// 读取 key 对应的非零标识；缺失、转换失败或为零均拒绝配置。
Id Identifier(const YAML::Node& node, const std::string& key) {
  auto value = node[key].as<Id>(0);
  if (value == 0) {
    throw std::runtime_error("invalid or missing " + key);
  }
  return value;
}

}  // namespace

Config LoadConfig(const std::string& path, const std::string& role) {
  auto yaml = YAML::LoadFile(path);
  Config config;
  if (yaml["role"].as<std::string>("") != role) {
    throw std::runtime_error("role must be " + role);
  }

  config.m_logEntries = Number(yaml, "log_entries");
  if (role != "game_worker") {
    return config;
  }

  config.m_playerShards = Number(yaml, "player_shards");
  config.m_sceneShards = Number(yaml, "scene_shards");
  config.m_computeThreads = Number(yaml, "compute_threads");

  auto queues = yaml["queues"];
  if (!queues.IsMap()) {
    throw std::runtime_error("missing queues");
  }

  unsigned index = 0;  // 与 Lane 枚举对应的预算下标。
  for (const auto* name : {"control", "input", "result", "background"}) {
    auto queue = queues[name];
    if (!queue.IsMap()) {
      throw std::runtime_error(std::string("missing queues.") + name);
    }
    try {
      config.m_budgets[index++] = {Number(queue, "max_entries"),
                                   Number(queue, "max_bytes"),
                                   Milliseconds(Number(queue, "max_age_ms")),
                                   Number(queue, "per_source")};
    } catch (const std::exception& error) {
      throw std::runtime_error(std::string("queues.") + name + ": " +
                               error.what());
    }
  }

  auto spaces = yaml["spaces"];
  if (!spaces.IsSequence()) {
    throw std::runtime_error("spaces must be a sequence");
  }

  std::set<SpaceId> bound;  // 已绑定的 Space 标识，用于拒绝重复分配。
  for (const auto& node : spaces) {
    SpaceConfig space{Identifier(node, "space_id"),
                      Number(node, "scene_shard", 0)};
    if (!bound.insert(space.m_id).second) {
      throw std::runtime_error("duplicate space_id");
    }
    if (space.m_shard >= config.m_sceneShards) {
      throw std::runtime_error("scene_shard outside scene_shards");
    }
    config.m_spaces.push_back(space);
  }
  return config;
}

}  // namespace pop
