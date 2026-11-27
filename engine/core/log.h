// 提供 LOG_INFO 和 LOG_ERROR 宏，供进程、执行分片和服务记录事件。
// 进程入口通过 StartLogging 和 StopLogging 管理日志生命周期。

#pragma once

#include <spdlog/spdlog.h>

#include <cstddef>
#include <string>

namespace pop {

// 安装默认异步日志器，返回本次进程启动的实例标识，用于日志与路由核对。
std::string StartLogging(const std::string& role, std::size_t entries);

// 在生产日志的线程退出后调用，关闭日志器并回收后台写入线程。
void StopLogging();

}  // namespace pop

// 用格式串和参数记录普通运行事件。
#define LOG_INFO(...) SPDLOG_INFO(__VA_ARGS__)

// 用格式串和参数记录错误原因。
#define LOG_ERROR(...) SPDLOG_ERROR(__VA_ARGS__)
