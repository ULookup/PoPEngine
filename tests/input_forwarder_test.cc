// 通过可控的 TLS 写入口验证网关按路由成批、覆盖输入及满载时的原请求回执。
#include "engine/net/input_forwarder.h"

#include <chrono>
#include <condition_variable>
#include <iostream>
#include <map>
#include <mutex>
#include <set>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "engine/net/wire_frame.h"

namespace pop {
namespace {

using namespace std::chrono_literals;

// 向 CTest 报告可观察结果的具体偏差。
void Check(bool condition, const char* message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

// 捕获实际编码的帧。首批占住发送线程，下一窗口由用例显式开放。
class BatchSink final : public TlsClient {
 public:
  // 暂停第一个批次；模拟 Space 51 满载，以及 Space 52 在转交时失去结果。
  SendStatus send(WireBytes bytes, std::optional<Id>) override {
    const auto envelope = DecodeFrame(bytes);
    wire::InputBatch batch;
    if (!envelope || envelope->message_type() != wire::INPUT_BATCH ||
        !batch.ParseFromString(envelope->payload())) {
      return SendStatus::TooLarge;
    }

    std::unique_lock lock(m_mutex);
    const auto space = batch.space_id();
    m_frames.push_back(std::move(batch));
    m_changed.notify_all();
    if (m_frames.size() == 1) {
      m_changed.wait_for(lock, 2s, [this] { return m_released; });
    }
    if (space == 52) {
      throw std::runtime_error("transport result unknown");
    }
    return space == 51 ? SendStatus::Full : SendStatus::Queued;
  }

  // 等到第 count 批真正提交给 TLS，并复制该帧供断言。
  wire::InputBatch frame(std::size_t count) {
    std::unique_lock lock(m_mutex);
    Check(
        m_changed.wait_for(lock, 2s, [&] { return m_frames.size() >= count; }),
        "timed out waiting for the expected TLS batch");
    return m_frames.at(count - 1);
  }

  // 安排好的输入都进入同一窗口后，允许后台写线程取件。
  void release() {
    std::lock_guard lock(m_mutex);
    m_released = true;
    m_changed.notify_all();
  }

 private:
  std::mutex m_mutex;                      // 保护已观察到的帧和写端状态。
  std::condition_variable m_changed;       // 发帧或释放暂停后通知等待方。
  std::vector<wire::InputBatch> m_frames;  // 实际交给 TLS 的帧。
  bool m_released = false;                 // 首个写操作是否可以返回。
};

// 构造有充分有效期的独立输入；截止已过的场景另外显式设置。
wire::RoutedInput Input(Id request, Id entity, Id sequence) {
  wire::RoutedInput input;
  input.set_request_id(request);
  input.set_entity_id(entity);
  input.set_session_epoch(1);
  input.set_entity_generation(1);
  input.set_input_seq(sequence);
  input.set_state("x");
  input.set_expires_at_unix_ms(UnixMilliseconds() + INPUT_LIFETIME.count());
  return input;
}

// 稳定地占住首批，检查并发到达的不同实体、同绑定及另一个 Space。
void CheckBatchWindow() {
  BatchSink sink;
  std::mutex mutex;
  std::condition_variable changed;
  std::map<Id, wire::InputStatus> results;
  NetLimits limits;
  limits.m_queueEntries = 4;
  limits.m_queueBytes = 4096;
  InputForwarder forwarder(
      sink, limits, [&](Id request, Id, SpaceId, wire::InputStatus status) {
        std::lock_guard lock(mutex);
        results.emplace(request, status);
        changed.notify_all();
      });

  forwarder.post(40, 1, "worker", Input(1, 9, 1));
  sink.frame(1);
  forwarder.post(50, 2, "next-worker", Input(2, 10, 1));
  forwarder.post(50, 2, "next-worker", Input(3, 11, 1));
  forwarder.post(50, 2, "next-worker", Input(4, 10, 2));
  forwarder.post(50, 2, "next-worker", Input(5, 10, 1));
  forwarder.post(51, 1, "worker", Input(6, 10, 1));
  forwarder.post(50, 2, "next-worker", Input(7, 12, 1));
  auto large = Input(8, 10, 3);
  large.set_state(std::string(4096, 'x'));
  forwarder.post(50, 2, "next-worker", std::move(large));
  auto expired = Input(9, 10, 4);
  expired.set_expires_at_unix_ms(1);
  forwarder.post(50, 2, "next-worker", std::move(expired));
  sink.release();

  auto frame = sink.frame(2);
  Check(frame.space_id() == 50 && frame.space_owner_epoch() == 2 &&
            frame.worker_instance_id() == "next-worker",
        "batch used a different Space or owner");
  std::set<Id> requests;
  for (const auto& input : frame.inputs()) {
    requests.insert(input.request_id());
  }
  Check(frame.inputs_size() == 2 && requests == std::set<Id>{3, 4},
        "latest inputs for two independent entities were not batched");
  Check(sink.frame(3).space_id() == 51, "input from another Space was mixed");
  forwarder.post(52, 1, "worker", Input(10, 10, 1));

  std::unique_lock lock(mutex);
  Check(changed.wait_for(lock, 2s, [&] { return results.contains(10); }),
        "transport failure did not release the associated request");
  Check(results == std::map<Id, wire::InputStatus>{{2, wire::SUPERSEDED},
                                                   {5, wire::STALE},
                                                   {6, wire::BUSY},
                                                   {7, wire::BUSY},
                                                   {8, wire::BUSY},
                                                   {9, wire::EXPIRED},
                                                   {10, wire::UNKNOWN}},
        "incorrect receipts for merged, expired, stale or rejected inputs");
}

}  // namespace
}  // namespace pop

// 将具体批处理断言的异常交给 CTest。
int main() {
  try {
    pop::CheckBatchWindow();
  } catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return 1;
  }
}
