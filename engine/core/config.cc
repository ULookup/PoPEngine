// 解析服务端进程配置，在启动前校验角色、队列预算和 Space 放置。

#include "engine/core/config.h"

#include <yaml-cpp/yaml.h>

#include <limits>
#include <set>
#include <stdexcept>
#include <utility>

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

// 读取必要字符串；报错时只记录键名，保护凭据内容。
std::string RequiredText(const YAML::Node& node, const char* key) {
  const auto value = node[key].as<std::string>("");
  if (value.empty()) {
    throw std::runtime_error(std::string("missing network field ") + key);
  }
  return value;
}

// 检查 TCP/UDP 端口后才收窄为传输库的整数类型。
Endpoint ReadEndpoint(const YAML::Node& node) {
  const auto port = Number(node, "port");
  if (port > 65535) {
    throw std::runtime_error("network port outside 1..65535");
  }
  return {RequiredText(node, "host"), static_cast<std::uint16_t>(port)};
}

// 内部链路必须同时配置可信 CA、对端身份与本端密钥对。
TlsFiles ReadTls(const YAML::Node& node, bool mutual) {
  return {
      .m_ca = mutual ? RequiredText(node, "ca") : "",
      .m_certificate = RequiredText(node, "certificate"),
      .m_key = RequiredText(node, "key"),
      .m_peerName = mutual ? RequiredText(node, "peer_name") : "",
  };
}

// 验收身份只允许装配在明确的回环地址上，避免 DNS 变化扩大信任范围。
void RequireLoopback(const Endpoint& endpoint) {
  if (endpoint.m_host != "127.0.0.1" && endpoint.m_host != "::1") {
    throw std::runtime_error("isolated_fixture requires a loopback address");
  }
}

// 验收绑定从配置读取，网关无需信任客户端声称的归属信息。
FixtureBinding ReadBinding(const YAML::Node& node) {
  return {Identifier(node, "space_id"),
          {.m_entity = Identifier(node, "entity_id"),
           .m_generation = Identifier(node, "entity_generation"),
           .m_sessionEpoch = Identifier(node, "session_epoch")}};
}

// 按进程角色加载所需端点，缺少 network 段时沿用空载启动契约。
NetworkConfig ReadNetwork(const YAML::Node& net, const std::string& role) {
  NetworkConfig config;
  if (!net) {
    return config;
  }
  if (!net.IsMap()) {
    throw std::runtime_error("network must be a mapping");
  }
  if (const auto limits = net["limits"]) {
    config.m_limits = {
        .m_maxConnections = Number(limits, "max_connections"),
        .m_queueEntries = Number(limits, "queue_entries"),
        .m_queueBytes = Number(limits, "queue_bytes"),
        .m_timeout = Milliseconds(Number(limits, "timeout_ms")),
    };
  }

  const auto fixture = net["isolated_fixture"];
  if (role == "gateway") {
    GatewayNetwork gateway{ReadEndpoint(net["ingress"]),
                           ReadEndpoint(net["scene"]),
                           ReadTls(net["public_tls"], false),
                           ReadTls(net["tls"], true),
                           {}};
    if (fixture) {
      RequireLoopback(gateway.m_ingress);
      RequireLoopback(gateway.m_scene);
      gateway.m_route = FixtureRoute{
          ReadBinding(fixture), RequiredText(fixture, "worker_instance_id"),
          RequiredText(fixture, "token_file")};
    }
    config.m_service = std::move(gateway);
  } else if (role == "game_worker") {
    if (config.m_limits.m_maxConnections < 2) {
      throw std::runtime_error(
          "Scene needs separate input and state connections");
    }
    SceneNetwork scene{
        ReadEndpoint(net["scene"]), ReadTls(net["tls"], true), {}};
    if (fixture) {
      RequireLoopback(scene.m_endpoint);
      scene.m_fixture = ReadBinding(fixture);
    }
    config.m_service = std::move(scene);
  } else if (role == "platform") {
    config.m_service = PlatformNetwork{ReadEndpoint(net["http"])};
  } else if (role == "control") {
    config.m_service =
        ControlNetwork{ReadEndpoint(net["rpc"]), ReadTls(net["tls"], true)};
  }
  return config;
}

}  // namespace

Config LoadConfig(const std::string& path, const std::string& role) {
  auto yaml = YAML::LoadFile(path);
  Config config;
  if (yaml["role"].as<std::string>("") != role) {
    throw std::runtime_error("role must be " + role);
  }

  config.m_logEntries = Number(yaml, "log_entries");
  config.m_network = ReadNetwork(yaml["network"], role);
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
