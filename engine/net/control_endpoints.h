// 装配 Platform 的内网 HTTP 健康端点与 Control 的双向认证健康查询。
#pragma once

#include <memory>

#include "engine/net/transport.h"

namespace pop {

// 监听 Platform 的 GET /healthz，限制在途连接数与每次读写的时限。
std::unique_ptr<NetworkListener> StartPlatformHealth(Endpoint endpoint,
                                                     NetLimits limits);

// 监听标准 gRPC Health 服务，仅接受受信 CA 签发且身份匹配的调用方证书。
std::unique_ptr<NetworkListener> StartControlRpc(Endpoint endpoint,
                                                 TlsFiles files,
                                                 NetLimits limits);

}  // namespace pop
