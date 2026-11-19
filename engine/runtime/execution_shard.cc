// 在分片线程内创建 Lua VM 并按截止点驱动执行循环。
// 初始化异常返回启动方，运行期间的异常交给故障回调。

#include "engine/runtime/execution_shard.h"

#include <condition_variable>
#include <future>
#include <mutex>
#include <stop_token>
#include <utility>

#include "engine/core/log.h"
#include "engine/script/lua_vm.h"

namespace pop {
std::jthread StartExecutionShard(
    std::string service, unsigned index, std::function<ShardStep()> initialize,
    std::function<void(std::exception_ptr)> on_failure) {
  // initialized 由分片线程写入；启动方通过 ready 等待就绪或取得启动异常。
  std::promise<void> initialized;
  auto ready = initialized.get_future();
  std::jthread thread(
      [service = std::move(service), index, initialize = std::move(initialize),
       on_failure = std::move(on_failure),
       initialized = std::move(initialized)](std::stop_token stop) mutable {
        bool running = false;  // 就绪已上报后，异常改由运行故障回调处理。
        try {
          auto vm = CreateLuaVm();
          auto step = initialize();
          // wake 和对应的锁只用于可被停止请求打断的休眠。
          std::mutex mutex;
          std::condition_variable_any wake;
          std::unique_lock lock(mutex);
          LOG_INFO("event=shard_ready service={} shard={}", service, index);
          initialized.set_value();
          running = true;
          while (!stop.stop_requested()) {
            auto deadline = step ? step(Clock::now()) : TimePoint::max();
            if (deadline == TimePoint::max())
              wake.wait(lock, stop, [] { return false; });
            else
              wake.wait_until(lock, stop, deadline, [] { return false; });
          }
          LOG_INFO("event=shard_stop service={} shard={}", service, index);
        } catch (...) {
          if (running)
            on_failure(std::current_exception());
          else
            initialized.set_exception(std::current_exception());
        }
      });
  ready.get();
  return thread;
}
}  // namespace pop
