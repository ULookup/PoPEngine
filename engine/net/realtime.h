// 将两种公网传输接入同一网关路由，并将内部输入批次投递到 GameWorker。
#pragma once

#include <memory>
#include <string>

#include "engine/net/settings.h"

namespace pop {
class GameWorker;

// 启动到 Scene 的 mTLS 长连接和两个公网监听器，持有全部网关回调。
std::unique_ptr<NetworkListener> StartRealtimeGateway(
    const GatewayNetwork& config, NetLimits limits);

// 在配置的 Space 安装可选验收实体，随后开始接收发往本实例的输入。
std::unique_ptr<NetworkListener> StartSceneIngress(const SceneNetwork& config,
                                                   NetLimits limits,
                                                   const std::string& instance,
                                                   GameWorker& worker);

}  // namespace pop
