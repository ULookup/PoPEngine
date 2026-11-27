// 提供独立于输入和回执的普通状态连接，按隔离身份查询 Scene 已发布的整步统计。
#pragma once

#include <memory>
#include <optional>
#include <string>

#include "engine/net/settings.h"
#include "realtime.pb.h"

namespace pop {

class GameWorker;

// 网关持有一条专用 mTLS 连接，按内网请求号将采样结果送回原观察连接。
class StateChannel {
 public:
  // 回收通道及其所有在途查询。
  virtual ~StateChannel() = default;

  // 为已验证身份查询其 Space；query_id 由调用连接解释。
  virtual void query(Id query_id, const FixtureRoute& route,
                     SendFrame reply) = 0;
};

// 建立普通状态专用连接和有界回送表，独立执行重连与超时处理。
std::unique_ptr<StateChannel> StartStateChannel(const GatewayNetwork& config,
                                                NetLimits limits);

// 向原观察连接返回无快照的查询结果，使用与状态数据相同的传输类别。
void ReplyStateStatus(const SendFrame& reply, Id query_id, SpaceId space,
                      wire::StateStatus status);

// 在 Worker 核对实例、承载代次及隔离身份，复制最近一次发布的快照。
void ServeStateQuery(const wire::Envelope& envelope,
                     const std::string& instance,
                     const std::optional<FixtureBinding>& fixture,
                     const GameWorker& worker, const SendFrame& send);

}  // namespace pop
