// 通过正式数据接口验证隔离计数操作，故障时序由外部隔离驱动控制。

#include <chrono>
#include <cstdlib>
#include <functional>
#include <future>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>

#include "engine/cache/sample_read_view.h"
#include "messaging/sample_messaging.h"
#include "services/game_worker.h"
#include "storage/database_executor.h"

namespace pop {

// 验证无路由及隔离重放，沿用本次测试的专用分片和 Broker。
void CheckMessagingRecovery(SampleRepository& repository,
                            const BrokerSettings& settings);

namespace {

using namespace std::chrono_literals;

// 整个验收跨进程沿用相同操作号与聚合，第二、三笔观察版本递增。
constexpr SampleCommand FIRST{
    .m_operationId = 101, .m_aggregateId = 42, .m_delta = 3};
// 第二笔并发重试，共增加五次计数。
constexpr SampleCommand SECOND{
    .m_operationId = 102, .m_aggregateId = 42, .m_delta = 5};
// 快照重建期间到达的新操作。
constexpr SampleCommand THIRD{
    .m_operationId = 103, .m_aggregateId = 42, .m_delta = 4};

// 读取隔离驱动设置的参数，诊断只包含参数名。
std::string Env(const char* name) {
  const auto* value = std::getenv(name);
  if (!value) throw std::runtime_error(std::string("missing ") + name);
  return value;
}

// 检查对调用方可见的结果，失败时保留场景说明。
void Require(bool condition, const char* reason) {
  if (!condition) throw std::runtime_error(reason);
}

// 经有界执行池提交一次命令，回调将独立的值交给驱动线程。
std::future<SampleResult> Submit(DatabaseExecutor& executor,
                                 SampleCommand command,
                                 TimePoint deadline = Clock::now() + 10s) {
  auto reply = std::make_shared<std::promise<SampleResult>>();
  auto future = reply->get_future();
  const bool accepted = executor.postApply(
      command, deadline,
      std::make_shared<std::function<void(SampleResult)>>(
          [reply](SampleResult result) { reply->set_value(result); }));
  Require(accepted, "database task rejected at admission");
  return future;
}

// 只在驱动线程按有界时间等待已受理消息的完成结果。
template <typename T>
T Await(std::future<T>& future) {
  Require(future.wait_for(12s) == std::future_status::ready,
          "timed out waiting for completion");
  return future.get();
}

// 确认原请求可恢复同一权威事件和值。
void CheckResult(const SampleResult& result, std::uint64_t operation_id,
                 std::uint64_t version, std::uint64_t value) {
  Require(result.m_status == SampleStatus::Committed,
          "operation not committed");
  const auto& event = result.m_event;
  Require(event.m_operationId == operation_id && event.m_eventId != 0 &&
              event.m_aggregateId == FIRST.m_aggregateId &&
              event.m_version == version && event.m_value == value,
          "wrong durable operation, aggregate, version or value");
}

// 收到真实数据库锁等待的通知后，通过同进程的 Scene 取得新的提交 Tick。
void CheckSceneProgress(GameWorker& worker) {
  std::string signal;
  Require(std::getline(std::cin, signal) && signal == "locked",
          "missing database lock barrier");
  constexpr SpaceId SPACE = 101;
  const auto start_tick = worker.snapshot(SPACE).value().m_tick;
  constexpr EntityBinding BINDING{
      .m_entity = 7, .m_generation = 1, .m_sessionEpoch = 1};
  BindEntity bind{BINDING, {}};
  auto bound = bind.m_reply.get_future();
  const auto admitted = worker.bindEntity(SPACE, std::move(bind));
  Require(admitted == Admission::Accepted, "Scene rejected binding");
  Require(Await(bound) == RequestStatus::Completed, "binding failed");

  auto reply = std::make_shared<std::promise<Tick>>();
  auto applied = reply->get_future();
  ControlInput input{BINDING, 1, {}, Clock::now() + 1s};
  const auto accepted = worker.postInput(
      SPACE, std::move(input), {}, [reply](InputResult result, Tick tick) {
        reply->set_value(result == InputResult::Applied ? tick : 0);
      });
  Require(accepted == Admission::Accepted, "Scene rejected input");
  const auto end_tick = Await(applied);
  Require(end_tick > start_tick, "Scene did not commit during lock wait");
  std::cout << "space=101 start_tick=" << start_tick
            << " committed_tick=" << end_tick << '\n';
}

// 保持首个回包丢失，走原操作查询和并发重试，两笔命令合计只增加八次计数。
void Write(SampleRepository& repository) {
  GameWorker worker(LoadConfig(Env("POP_WORKER_CONFIG"), "game_worker"));
  DatabaseExecutor executor(repository, 3, 8, 8192);
  auto dropped = std::make_shared<std::promise<void>>();
  auto finished = dropped->get_future();
  const auto accepted = executor.postApply(
      FIRST, Clock::now() + 15s,
      std::make_shared<std::function<void(SampleResult)>>(
          [dropped](SampleResult) {
            dropped->set_value();  // 仅同步故障注入，主动丢弃整个响应。
          }));
  Require(accepted, "first operation refused before submission");
  constexpr SampleCommand EXPIRING{
      .m_operationId = 104, .m_aggregateId = 42, .m_delta = 11};
  const auto deadline = Clock::now() + 2s;
  auto expiring = Submit(executor, EXPIRING, deadline);

  CheckSceneProgress(worker);
  Require(expiring.wait_until(deadline) == std::future_status::timeout,
          "expiring operation did not wait for the row lock");
  Require(finished.wait_for(0s) == std::future_status::timeout,
          "database completed before fixture released row lock");
  std::cout << "SCENE_ADVANCED" << std::endl;
  Await(finished);
  const auto expired = Await(expiring);
  const auto rolled_back = repository.query(EXPIRING.m_operationId);
  Require(expired.m_status == SampleStatus::Rejected &&
              rolled_back.m_status == SampleStatus::Missing,
          "expired uncommitted operation left a durable effect");

  auto reply = std::make_shared<std::promise<SampleResult>>();
  auto queried = reply->get_future();
  const auto queried_ok = executor.postQuery(
      FIRST.m_operationId, Clock::now() + 5s,
      std::make_shared<std::function<void(SampleResult)>>(
          [reply](SampleResult result) { reply->set_value(result); }));
  Require(queried_ok, "operation query refused");
  const auto recovered = Await(queried);
  CheckResult(recovered, FIRST.m_operationId, 1, 3);
  auto retried = Submit(executor, FIRST);
  const auto replay = Await(retried);
  CheckResult(replay, FIRST.m_operationId, 1, 3);
  Require(replay.m_event.m_eventId == recovered.m_event.m_eventId,
          "retry changed the original event identity");

  auto first_try = Submit(executor, SECOND);
  auto second_try = Submit(executor, SECOND);
  const auto a = Await(first_try);
  const auto b = Await(second_try);
  CheckResult(a, SECOND.m_operationId, 2, 8);
  CheckResult(b, SECOND.m_operationId, 2, 8);
  auto conflicting = FIRST;
  conflicting.m_delta = 9;
  auto conflict = Submit(executor, conflicting);
  const auto rejected = Await(conflict);
  Require(rejected.m_status == SampleStatus::Rejected,
          "reused operation ID accepted different arguments");
}

// 等待数据库中延迟认领的事件到期，遇到集成环境异常立即停止。
void DrainOutbox(SampleMessaging& broker, SampleRepository& repository) {
  unsigned published = 0;
  const auto end = Clock::now() + 12s;
  while (published < 2 && Clock::now() < end) {
    const auto result = broker.relayOne(repository);
    if (result == RelayResult::Published) {
      ++published;
    } else {
      Require(result == RelayResult::Idle,
              "relay failed after broker recovery");
      std::this_thread::sleep_for(20ms);
    }
  }
  Require(published == 2, "durable outbox failed to resume");
}

// 处理当前订阅的一次投递，等待队列可见性并设置明确截止。
ConsumeResult Consume(SampleMessaging& broker, SampleRepository& repository,
                      SampleReadView& view) {
  const auto end = Clock::now() + 5s;
  do {
    const auto result = broker.consumeOne(
        repository, [&view](const auto& e) { view.install(e); });
    if (result != ConsumeResult::Empty) return result;
    std::this_thread::sleep_for(20ms);
  } while (Clock::now() < end);
  throw std::runtime_error("timed out waiting for quorum delivery");
}

// 读取缓存版本和值，避免把未安装的投影视为合法的零值。
void CheckView(SampleReadView& view, std::uint64_t version,
               std::uint64_t value) {
  const auto state = view.read(FIRST.m_aggregateId);
  Require(state && state->m_version == version && state->m_value == value,
          "Redis view has wrong version or value");
}

// 从停机期间积累的 Outbox 恢复投递，然后让同一持久事件再次到达订阅。
void Deliver(SampleMessaging& broker, SampleRepository& repository,
             SampleReadView& view) {
  DrainOutbox(broker, repository);
  const auto first = Consume(broker, repository, view);
  const auto second = Consume(broker, repository, view);
  Require(first == ConsumeResult::Applied && second == ConsumeResult::Applied,
          "missing committed inbox effect");
  CheckView(view, 2, 8);

  const auto original = repository.query(FIRST.m_operationId);
  CheckResult(original, FIRST.m_operationId, 1, 3);
  Require(broker.publish(original.m_event), "duplicate publish failed");
  const auto duplicate = Consume(broker, repository, view);
  const auto count = repository.projectionApplications(FIRST.m_aggregateId);
  Require(duplicate == ConsumeResult::Duplicate && count == 2,
          "redelivery caused a duplicate business effect");
  CheckView(view, 2, 8);
  const auto checkpoint = view.beginRebuild(repository, FIRST.m_aggregateId);
  Require(checkpoint == 2, "wrong checkpoint before cache loss");
}

// 在清空的 Redis 中安装快照，再补齐期间的新事件并投递一条旧版本。
void Rebuild(SampleRepository& repository, SampleReadView& view) {
  const auto id = FIRST.m_aggregateId;
  Require(!view.read(id), "fixture did not clear Redis");
  // 上一进程已保存水位 2；即使中间没有新提交也须恢复缓存。
  const auto resumed = view.finishRebuild(repository, id, 2);
  Require(resumed == 2, "resumed rebuild lost its saved watermark");
  CheckView(view, 2, 8);
  const auto watermark = view.beginRebuild(repository, id);
  Require(watermark == 2, "wrong rebuild snapshot watermark");
  CheckView(view, 2, 8);
  const auto next = repository.apply(THIRD);
  CheckResult(next, THIRD.m_operationId, 3, 12);
  const auto high_water = view.finishRebuild(repository, id, watermark);
  Require(high_water == 3, "rebuild missed a concurrent durable write");

  const auto original = repository.query(FIRST.m_operationId);
  CheckResult(original, FIRST.m_operationId, 1, 3);
  const auto installed = view.install(original.m_event);
  Require(!installed, "stale event overwrote rebuilt cache");
  CheckView(view, 3, 12);
}

// 模拟旧 Relay 延迟到达的确认，验证重新认领后的生产者仍可投递原事件。
void CheckClaimGeneration(SampleRepository& repository) {
  const auto old_claim = repository.claimOutbox(5s);
  Require(old_claim && old_claim->m_event.m_operationId == THIRD.m_operationId,
          "missing outstanding rebuild event");
  Require(repository.deferOutbox(*old_claim, 0ms, false),
          "failed to release original claim");
  const auto current = repository.claimOutbox(5s);
  Require(current &&
              current->m_event.m_eventId == old_claim->m_event.m_eventId &&
              current->m_generation != old_claim->m_generation,
          "recovery changed event identity or reused claim generation");
  const bool stale = repository.markPublished(*old_claim);
  const bool released = repository.deferOutbox(*current, 0ms, false);
  Require(!stale && released, "stale acknowledgement finished the new claim");
}

// 通过隔离驱动提供的地址和凭据装配样例订阅、数据库与 Redis 客户端。
void Run(const std::string& action) {
  SampleRepository repository({
      .m_user = Env("POP_DB_USER"),
      .m_password = Env("POP_DB_PASSWORD"),
      .m_database = Env("POP_DB_NAME"),
      .m_host = Env("POP_DB_HOST"),
      .m_port = static_cast<unsigned>(std::stoul(Env("POP_DB_PORT"))),
  });
  if (action == "write") return Write(repository);
  const BrokerSettings settings{.m_uri = Env("POP_AMQP_URI")};
  SampleMessaging broker(settings);
  if (action == "topology") return broker.declareTopology();
  if (action == "offline") {
    const auto result = broker.relayOne(repository);
    Require(result == RelayResult::Deferred, "offline publish lost its retry");
    return;
  }
  SampleReadView view({
      .m_host = Env("POP_REDIS_HOST"),
      .m_port = std::stoi(Env("POP_REDIS_PORT")),
      .m_password = Env("POP_REDIS_PASSWORD"),
  });
  if (action == "deliver") return Deliver(broker, repository, view);
  if (action == "rebuild") {
    Rebuild(repository, view);
    CheckClaimGeneration(repository);
    CheckMessagingRecovery(repository, settings);
    return;
  }
  throw std::invalid_argument("unknown data scenario");
}

}  // namespace
}  // namespace pop

// 运行命令行指定的阶段三验收步骤，失败时告知编排器。
int main(int argc, char** argv) {
  try {
    if (argc != 2) return 2;
    pop::Run(argv[1]);
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "data driver: " << error.what() << '\n';
    return 1;
  }
}
