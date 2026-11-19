// 按固定步长推进分片内的各个 Space，采纳实体输入并发布运行统计。
// 计算结果在所属分片核对任务身份与版本后提交。

#include "engine/scene/scene_shard.h"

#include <algorithm>
#include <stdexcept>
#include <utility>

#include "engine/core/log.h"

namespace pop {
SpaceChannel::SpaceChannel(SpaceConfig config, Budgets budgets)
    : m_config(config), m_mailbox(budgets) {}
Snapshot SpaceChannel::snapshot() const {
  // 读锁与发布共用同一互斥量，保证一次复制能读到完整的一步。
  std::lock_guard lock(m_mutex);
  return m_snapshot;
}
void SpaceChannel::publish(const Snapshot& snapshot) {
  std::lock_guard lock(m_mutex);
  m_snapshot = snapshot;
}

SceneShard::SceneShard(Dispatch dispatch) : m_dispatch(std::move(dispatch)) {}
void SceneShard::addSpace(SpaceChannel& channel, TimePoint now,
                          std::unique_ptr<SpaceSimulation> simulation) {
  if (!m_spaces
           .emplace(channel.m_config.m_id,
                    Space{channel, now + TICK_STEP, std::move(simulation)})
           .second)
    throw std::logic_error("duplicate Space on Scene shard");
}

RequestStatus SceneShard::bindEntity(Space& space, EntityBinding binding) {
  auto [it, inserted] =
      space.m_entities.try_emplace(binding.m_entity, Entity{binding});
  const auto& current = it->second.m_binding;
  if (inserted || current == binding) return RequestStatus::Completed;
  if (binding.m_generation < current.m_generation ||
      (binding.m_generation == current.m_generation &&
       binding.m_sessionEpoch < current.m_sessionEpoch))
    return RequestStatus::StaleBinding;
  // 同一实体重绑会话时延续状态版本，并清除旧序号、控制和待提交任务。
  const Id version = binding.m_generation == current.m_generation
                         ? it->second.m_version + 1
                         : 1;
  it->second = Entity{binding, version};
  return RequestStatus::Completed;
}

void SceneShard::receiveResults(Space& space, TimePoint now) {
  auto& channel = space.m_channel;
  auto batch = channel.m_mailbox.take(Lane::Result, now, now);
  // 按实体标识派发，同一窗口内的提交顺序与计算线程的完成先后无关。
  std::stable_sort(
      batch.begin(), batch.end(),
      [](const Delivery& left, const Delivery& right) {
        return std::get<WorkResult>(left.m_message.m_payload).m_key.m_entity <
               std::get<WorkResult>(right.m_message.m_payload).m_key.m_entity;
      });
  for (const auto& item : batch) {
    const auto& result = std::get<WorkResult>(item.m_message.m_payload);
    const auto& key = result.m_key;
    auto it = space.m_entities.find(key.m_entity);
    const char* reason = nullptr;  // 非空时记录丢弃原因并跳过提交。
    if (item.m_expired)
      reason = "expired";
    else if (it == space.m_entities.end() || !it->second.m_pending)
      reason = "no_pending_task";
    else if (key.m_version != it->second.m_version)
      reason = "version_mismatch";
    else if (key != *it->second.m_pending)
      reason = "task_mismatch";
    if (reason) {
      ++space.m_droppedResults;
      LOG_INFO(
          "event=result_discard shard={} space={} tick={} task_tick={} "
          "version={} reason={}",
          channel.m_config.m_shard, channel.m_config.m_id, space.m_state.m_tick,
          key.m_tick, key.m_version, reason);
      continue;
    }
    auto& entity = it->second;
    space.m_simulation->commit(entity.m_binding, key.m_stage, result.m_output);
    ++space.m_state.m_resultCommits;
    ++entity.m_version;
    entity.m_pending.reset();
  }
}

void SceneShard::receiveInputs(Space& space, TimePoint now) {
  auto& channel = space.m_channel;
  for (auto& item :
       channel.m_mailbox.take(Lane::Input, now, space.m_nextTick)) {
    auto& input = std::get<ControlInput>(item.m_message.m_payload);
    auto it = space.m_entities.find(input.m_binding.m_entity);
    if (it == space.m_entities.end() ||
        it->second.m_binding != input.m_binding ||
        input.m_seq <= it->second.m_inputSeq) {
      LOG_INFO(
          "event=input_discard space={} tick={} entity={} input_seq={} "
          "reason=invalid_binding_or_sequence",
          channel.m_config.m_id, space.m_state.m_tick, input.m_binding.m_entity,
          input.m_seq);
      continue;
    }
    auto& entity = it->second;
    entity.m_inputSeq = input.m_seq;
    ++entity.m_version;
    ++space.m_state.m_inputs;
    if (item.m_expired)
      entity.m_input.reset();
    else
      entity.m_input = std::move(input);
    LOG_INFO(
        "event=input_applied shard={} space={} tick={} entity={} input_seq={} "
        "status={} trace={}",
        channel.m_config.m_shard, channel.m_config.m_id, space.m_state.m_tick,
        entity.m_binding.m_entity, entity.m_inputSeq,
        item.m_expired ? "expired_stop" : "applied", item.m_message.m_trace);
  }
}

void SceneShard::advanceEntities(Space& space, TimePoint now) {
  const auto& config = space.m_channel.m_config;
  for (auto& [id, entity] : space.m_entities) {
    entity.m_pending.reset();  // 上一步的回投窗口到此关闭。
    if (entity.m_input && now >= entity.m_input->m_validUntil) {
      entity.m_input.reset();
      ++entity.m_version;
    }
    if (!space.m_simulation) continue;
    auto update = space.m_simulation->advance(
        entity.m_binding, space.m_state.m_tick,
        entity.m_input ? &*entity.m_input : nullptr);
    if (update.m_changed) ++entity.m_version;
    if (!update.m_work) continue;
    auto& request = *update.m_work;
    // 本地静态放置的首次承载代次为 1；任务只在本进程内流转。
    WorkKey key{
        config.m_id,      1,  space.m_state.m_tick,         request.m_stage,
        entity.m_version, id, entity.m_binding.m_generation};
    auto status = m_dispatch(
        space.m_channel,
        {key, std::move(request.m_snapshot), request.m_compute}, now);
    if (status == Admission::Accepted) {
      entity.m_pending = key;
      ++space.m_state.m_dispatched;
    } else {
      LOG_INFO("event=work_rejected space={} tick={} entity={} status={}",
               config.m_id, key.m_tick, id, Name(status));
    }
  }
}

void SceneShard::step(Space& space, TimePoint now) {
  const auto start = Clock::now();  // 本步实际开始时间，用于统计执行耗时。
  auto& channel = space.m_channel;
  const auto& config = channel.m_config;
  auto& state = space.m_state;
  auto controls = channel.m_mailbox.take(Lane::Control, now, space.m_nextTick);
  for (auto& item : controls) {
    if (auto* bind = std::get_if<BindEntity>(&item.m_message.m_payload))
      bind->m_reply.set_value(item.m_expired
                                  ? RequestStatus::Expired
                                  : bindEntity(space, bind->m_binding));
  }
  receiveResults(space, now);
  ++state.m_tick;
  receiveInputs(space, now);
  advanceEntities(space, now);
  state.m_entities = space.m_entities.size();
  channel.publish(state);
  for (auto& item : controls) {
    if (auto* request = std::get_if<Inspect>(&item.m_message.m_payload)) {
      request->m_reply.set_value(item.m_expired ? RequestStatus::Expired
                                                : RequestStatus::Completed);
      LOG_INFO("event=request_done space={} tick={} request_id={} expired={}",
               config.m_id, state.m_tick, request->m_requestId, item.m_expired);
    }
  }
  space.m_nextTick += TICK_STEP;
  if (state.m_tick == 1 || state.m_tick % 20 == 0) {
    // lag 是相对预定截止点的调度迟到量，elapsed 是本步已耗费的执行时间。
    auto lag = std::chrono::duration_cast<Milliseconds>(std::max(
        now - (space.m_nextTick - TICK_STEP), Clock::duration::zero()));
    auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
        Clock::now() - start);
    LOG_INFO(
        "event=tick shard={} space={} tick={} entities={} inputs={} "
        "dispatched={} commits={} dropped_results={} tick_lag_ms={} tick_us={}",
        config.m_shard, config.m_id, state.m_tick, state.m_entities,
        state.m_inputs, state.m_dispatched, state.m_resultCommits,
        space.m_droppedResults, lag.count(), elapsed.count());
    for (Lane lane :
         {Lane::Control, Lane::Input, Lane::Result, Lane::Background}) {
      const auto stats = channel.m_mailbox.stats(lane, now);
      LOG_INFO(
          "event=queue space={} lane={} entries={} bytes={} oldest_ms={} "
          "merged={} rejected={} full={} stale={} invalid={} "
          "expired_at_post={} closed={} expired_at_take={}",
          config.m_id, static_cast<int>(lane), stats.m_entries, stats.m_bytes,
          stats.m_oldestAge.count(), stats.m_merged, stats.m_rejected,
          stats.rejectedFor(Admission::Full),
          stats.rejectedFor(Admission::Stale),
          stats.rejectedFor(Admission::Invalid),
          stats.rejectedFor(Admission::Expired),
          stats.rejectedFor(Admission::Closed), stats.m_expired);
    }
  }
}

TimePoint SceneShard::advance(TimePoint now) {
  auto deadline = TimePoint::max();  // 各 Space 下一截止点中的最早时刻。
  for (auto& [id, space] : m_spaces) {
    int steps = 0;  // 本场在当前调度轮已推进的步数。
    while (steps < MAX_CATCH_UP && now >= space.m_nextTick) {
      step(space, now);
      ++steps;
    }
    if (steps == MAX_CATCH_UP) {
      // 达到单轮上限后仍已到期的步数，留给后续调度轮补算。
      const auto remaining = now >= space.m_nextTick
                                 ? (now - space.m_nextTick) / TICK_STEP + 1
                                 : 0;
      LOG_INFO(
          "event=catch_up shard={} space={} tick={} steps={} "
          "remaining_ticks={}",
          space.m_channel.m_config.m_shard, id, space.m_state.m_tick, steps,
          remaining);
    }
    deadline = std::min(deadline, space.m_nextTick);
  }
  return deadline;
}
}  // namespace pop
