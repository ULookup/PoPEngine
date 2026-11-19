// 配置 spdlog 有界异步队列，为日志附上角色和进程实例标识。
// 普通事件写入 stdout，错误写入 stderr。

#include "engine/core/log.h"

#include <spdlog/async.h>
#include <spdlog/sinks/stdout_sinks.h>

#include <fstream>
#include <memory>
#include <stdexcept>
#include <utility>

namespace pop {
namespace {
// 标准输出的日志接收器，将错误级别及以上的消息留给 stderr 接收器。
class InfoSink : public spdlog::sinks::stdout_sink_mt {
 public:
  // 将低于错误级别的消息转交给 spdlog 的线程安全 stdout 实现。
  void log(const spdlog::details::log_msg& message) override {
    if (message.level < spdlog::level::err) {
      spdlog::sinks::stdout_sink_mt::log(message);
    }
  }
};
}  // namespace

void StartLogging(const std::string& role, std::size_t entries) {
  std::string instance;
  std::ifstream uuid("/proc/sys/kernel/random/uuid");
  if (!(uuid >> instance))
    throw std::runtime_error("cannot obtain process instance UUID");

  auto output = std::make_shared<InfoSink>();
  auto errors = std::make_shared<spdlog::sinks::stderr_sink_mt>();
  errors->set_level(spdlog::level::err);
  spdlog::init_thread_pool(entries, 1);
  auto logger = std::make_shared<spdlog::async_logger>(
      role, spdlog::sinks_init_list{output, errors}, spdlog::thread_pool(),
      spdlog::async_overflow_policy::overrun_oldest);
  const char* field =
      role == "game_worker" ? " worker_instance_id=" : " instance_id=";
  const std::string context = std::string("role=%n") + field + instance;
  output->set_pattern(context + " %v");
  errors->set_pattern(context + " level=%l %v");
  logger->flush_on(spdlog::level::info);
  spdlog::set_default_logger(std::move(logger));
}

void StopLogging() { spdlog::shutdown(); }
}  // namespace pop
