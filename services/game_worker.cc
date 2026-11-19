// 装配 Player 与 Scene 分片，按 Space 路由输入，由计算线程回投任务结果。
// 停机时关闭投递入口、取消积压请求并回收线程。

#include "services/game_worker.h"

#include <signal.h>
#include <unistd.h>

#include <condition_variable>
#include <exception>
#include <map>
#include <mutex>
#include <stdexcept>
#include <stop_token>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "engine/core/log.h"
#include "engine/runtime/execution_shard.h"
#include "engine/scene/scene_shard.h"

namespace pop {
namespace {
// 跨 Space 轮流领取后台任务的计算池，工作线程将结果投回来源通道。
class ComputePool {
 public:
  // 借用由 Runtime 持有的通道，工作线程结束前它们必须保持有效。
  explicit ComputePool(std::vector<SpaceChannel*> channels)
      : m_channels(std::move(channels)) {}
  // 创建 threads 个计算线程，等待可领取的快照任务。
  void start(unsigned threads) {
    for (unsigned i = 0; i < threads; ++i)
      m_threads.emplace_back([this](std::stop_token stop) { run(stop); });
  }
  // 以 now 为任务创建时刻投递到 channel，受理后唤醒一个计算线程。
  Admission submit(SpaceChannel& channel, Work work, TimePoint now) {
    // 入队与唤醒共用扫描队列时的锁，避免线程在检查与休眠之间漏掉通知。
    std::lock_guard lock(m_mutex);
    // status 是当次入队结果；只有成功才需要通知正在等待的线程。
    auto status = channel.m_mailbox.post(
        {work.m_key.m_entity, now, std::move(work), {}}, Clock::now());
    if (status == Admission::Accepted) m_available.notify_one();
    return status;
  }
  // 向所有计算线程请求停止，并等待它们完成退出。
  void stop() {
    for (auto& thread : m_threads) thread.request_stop();
    m_threads.clear();
  }

 private:
  // 由计算线程运行；每次从一个 Space 取一批任务，在锁外计算并回投。
  void run(std::stop_token stop) {
    while (!stop.stop_requested()) {
      // channel 是本批任务的来源，也是结果回投的目的地；batch 持有取出的消息。
      SpaceChannel* channel = nullptr;
      std::vector<Delivery> batch;
      {
        std::unique_lock lock(m_mutex);
        m_available.wait(lock, stop, [&] {
          // 轮询各通道时使用同一取件时刻 now，最多扫描一遍通道表。
          auto now = Clock::now();
          for (std::size_t i = 0; i < m_channels.size(); ++i) {
            channel = m_channels[m_next];
            m_next = (m_next + 1) % m_channels.size();
            batch = channel->m_mailbox.take(Lane::Background, now, now);
            if (!batch.empty()) return true;
          }
          return false;
        });
      }
      for (const auto& item : batch) {
        if (item.m_expired) continue;
        const auto& work = std::get<Work>(item.m_message.m_payload);
        try {
          auto result = Compute(work);
          auto now = Clock::now();
          auto admission = channel->m_mailbox.post(
              {work.m_key.m_entity, now, std::move(result), {}}, now);
          if (admission != Admission::Accepted)
            LOG_INFO("event=result_enqueue space={} task_tick={} status={}",
                     work.m_key.m_space, work.m_key.m_tick, Name(admission));
        } catch (const std::exception& error) {
          LOG_ERROR("event=compute_failed space={} task_tick={} reason={}",
                    work.m_key.m_space, work.m_key.m_tick, error.what());
        }
      }
    }
  }
  std::vector<SpaceChannel*> m_channels;  // 可领取任务的 Space 通道列表。
  std::size_t m_next = 0;                 // 下次轮询从此通道下标开始。
  std::mutex m_mutex;  // 保护轮询游标并协调队列扫描、等待和唤醒。
  std::condition_variable_any m_available;  // 空闲线程等待任务或停止请求。
  std::vector<std::jthread> m_threads;      // 计算池拥有的工作线程。
};
}  // namespace

// GameWorker 的资源所有者，建立 Space 路由并协调各组线程的启停。
class GameWorker::Runtime {
 public:
  // 先建立全部 Space 通道，再启动计算池、Player 分片和 Scene 分片。
  Runtime(const Config& config, const SimulationFactory& create_simulation) {
    // channels 只借用通道；每个新 channel 的所有权交给按标识索引的路由表。
    std::vector<SpaceChannel*> channels;
    for (const auto& space : config.m_spaces) {
      auto channel = std::make_unique<SpaceChannel>(space, config.m_budgets);
      // it 定位实际存储的通道，inserted 表示该 Space 标识是否首次出现。
      auto [it, inserted] = m_channels.emplace(space.m_id, std::move(channel));
      if (!inserted) throw std::invalid_argument("duplicate space_id");
      channels.push_back(it->second.get());
    }
    m_compute = std::make_unique<ComputePool>(std::move(channels));
    m_compute->start(config.m_computeThreads);
    // 分片故障时先保存异常，再唤醒等待停机信号的进程主线程。
    auto failed = [this](std::exception_ptr failure) {
      std::lock_guard lock(m_failureMutex);
      m_failure = failure;
      kill(getpid(), SIGTERM);
    };
    for (unsigned i = 0; i < config.m_playerShards; ++i) {
      m_threads.push_back(StartExecutionShard(
          "PlayerService", i, [] { return ShardStep{}; }, failed));
    }
    for (unsigned i = 0; i < config.m_sceneShards; ++i) {
      m_threads.push_back(StartExecutionShard(
          "SceneService", i,
          [this, i, create_simulation] {
            return initializeScene(i, create_simulation);
          },
          failed));
    }
  }

  // 按只读 Space 路由表投递，以入口的单调时刻计量队列年龄。
  Admission post(SpaceId space_id, Id source, Payload payload,
                 std::string trace) {
    auto it = m_channels.find(space_id);
    if (it == m_channels.end()) return Admission::UnknownSpace;
    auto now = Clock::now();
    return it->second->m_mailbox.post(
        {source, now, std::move(payload), std::move(trace)}, now);
  }

  // 在所有者释放实例时完成资源回收。
  ~Runtime() { shutdown(); }
  // 完成停机，并将分片保存的异常重新抛给进程入口。
  void stop() {
    shutdown();
    if (m_failure) std::rethrow_exception(m_failure);
  }

 private:
  // 关闭投递，停止计算线程，取消队列积压，最后回收执行分片。
  void shutdown() {
    if (m_threads.empty()) return;
    for (auto& [id, channel] : m_channels) channel->m_mailbox.close();
    m_compute->stop();
    for (auto& [id, channel] : m_channels) {
      auto cancelled =
          channel->m_mailbox.cancelRemaining();  // 此场清除的条数。
      if (cancelled > 0)
        LOG_INFO("event=work_cancelled space={} entries={}", id, cancelled);
    }
    for (auto& thread : m_threads) thread.request_stop();
    m_threads.clear();
    for (auto& [id, channel] : m_channels)
      LOG_INFO("event=space_stop shard={} space={} tick={}",
               channel->m_config.m_shard, id, channel->snapshot().m_tick);
  }

  // 在分片 index 的线程上绑定所属 Space，返回持有该分片状态的步进回调。
  ShardStep initializeScene(unsigned index,
                            const SimulationFactory& create_simulation) {
    // 所有在本轮装配的 Space 从同一时刻计时，scene 最终由步进回调持有。
    auto scene = std::make_shared<SceneShard>(
        [this](SpaceChannel& channel, Work work, TimePoint now) {
          return m_compute->submit(channel, std::move(work), now);
        });
    auto now = Clock::now();
    for (auto& [id, channel] : m_channels) {
      if (channel->m_config.m_shard == index)
        scene->addSpace(*channel, now,
                        create_simulation ? create_simulation(id) : nullptr);
    }
    return [scene = std::move(scene)](TimePoint now) mutable {
      return scene->advance(now);
    };
  }
  // 按标识查找并拥有各场通道。资源按声明的逆序析构，即使后续分片
  // 启动失败，已启动的线程也先退出，最后才释放被它们借用的通道。
  std::map<SpaceId, std::unique_ptr<SpaceChannel>> m_channels;
  std::unique_ptr<ComputePool> m_compute;  // 为各 Scene 分片服务的计算池。
  std::mutex m_failureMutex;               // 串行化多个分片对运行异常的写入。
  std::exception_ptr m_failure;  // 分片记录的异常，等待线程结束后由 stop 报告。
  std::vector<std::jthread>
      m_threads;  // 本 Worker 的 Player 和 Scene 执行线程。
};

GameWorker::GameWorker(const Config& config,
                       SimulationFactory create_simulation)
    : m_runtime(std::make_unique<Runtime>(config, create_simulation)) {}
GameWorker::~GameWorker() = default;
void GameWorker::stop() { m_runtime->stop(); }
Admission GameWorker::bindEntity(SpaceId space_id, BindEntity request) {
  const auto source = request.m_binding.m_entity;
  return m_runtime->post(space_id, source, std::move(request), {});
}
Admission GameWorker::postInput(SpaceId space_id, ControlInput input,
                                std::string trace) {
  const auto source = input.m_binding.m_entity;
  return m_runtime->post(space_id, source, std::move(input), std::move(trace));
}
}  // namespace pop
