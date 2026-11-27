// 为 Gateway 建立 MsQuic 公网数据报入口和可靠回执流。
#pragma once

#include <memory>

#include "engine/net/transport.h"

namespace pop {

// 在指定 IP 和 UDP 端口监听；析构时停止接受连接并等待全部回调结束。
std::unique_ptr<NetworkListener> StartQuicListener(Endpoint endpoint,
                                                   TlsFiles files,
                                                   NetLimits limits,
                                                   ReceiveFrame receive);

}  // namespace pop
