// 声明执行分片的启动接口。步进回调返回下一次唤醒的单调时钟截止点。

#pragma once

#include <exception>
#include <functional>
#include <string>
#include <thread>

#include "engine/core/types.h"

namespace pop {

// 在分片线程上执行一次调度，接收当前时刻，返回下次截止点。
// 空回调表示该分片空闲，可以等待停机信号。
using ShardStep = std::function<TimePoint(TimePoint)>;

// 为 service 的第 index 个分片启动线程，在该线程创建 VM 并执行 initialize。
// 启动失败时抛出异常，启动成功后将线程交给调用方，运行异常交给 on_failure。
std::jthread StartExecutionShard(
    std::string service, unsigned index, std::function<ShardStep()> initialize,
    std::function<void(std::exception_ptr)> on_failure);

}  // namespace pop
