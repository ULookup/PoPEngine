// 在工作线程执行完整数据库事务。完成回调仅接收值，不能访问 Scene 可变内存。

#include "storage/database_executor.h"

#include <algorithm>
#include <stdexcept>
#include <utility>

#include "engine/core/log.h"

namespace pop {

DatabaseExecutor::DatabaseExecutor(SampleRepository& repository,
                                   std::size_t threads, std::size_t max_entries,
                                   std::size_t max_bytes)
    : m_repository(repository),
      m_queueLimit(std::min(max_entries, max_bytes / sizeof(Task))) {
  if (threads == 0 || max_entries == 0 || max_bytes == 0) {
    throw std::invalid_argument("invalid database executor budget");
  }
  m_threads.reserve(threads);
  try {
    for (std::size_t i = 0; i < threads; ++i) {
      m_threads.emplace_back(&DatabaseExecutor::run, this);
    }
  } catch (...) {
    shutdown();
    throw;
  }
}

DatabaseExecutor::~DatabaseExecutor() { shutdown(); }

bool DatabaseExecutor::postApply(SampleCommand command, TimePoint deadline,
                                 DatabaseCompletion completion) {
  return post({.m_operation = Operation::Apply,
               .m_command = command,
               .m_deadline = deadline,
               .m_completion = std::move(completion)});
}

bool DatabaseExecutor::postQuery(std::uint64_t operation_id, TimePoint deadline,
                                 DatabaseCompletion completion) {
  return post({.m_operation = Operation::Query,
               .m_command = {.m_operationId = operation_id,
                             .m_aggregateId = 0,
                             .m_delta = 0},
               .m_deadline = deadline,
               .m_completion = std::move(completion)});
}

bool DatabaseExecutor::post(Task task) {
  {
    std::lock_guard lock(m_mutex);
    if (m_closed || m_queue.size() >= m_queueLimit || !task.m_completion ||
        !*task.m_completion || Clock::now() >= task.m_deadline) {
      return false;
    }
    m_queue.push_back(std::move(task));
  }
  m_wakeup.notify_one();
  return true;
}

void DatabaseExecutor::run() {
  for (;;) {
    Task task;
    {
      std::unique_lock lock(m_mutex);
      m_wakeup.wait(lock, [this] { return m_closed || !m_queue.empty(); });
      if (m_queue.empty()) {
        return;
      }
      task = std::move(m_queue.front());
      m_queue.pop_front();
    }

    SampleResult result{SampleStatus::Rejected, {}};
    if (Clock::now() < task.m_deadline) {
      result = task.m_operation == Operation::Apply
                   ? m_repository.apply(task.m_command, task.m_deadline)
                   : m_repository.query(task.m_command.m_operationId);
    }
    try {
      (*task.m_completion)(result);
    } catch (...) {
      // 回投失败后，接收方仍可凭命令原标识查询。继续服务其他任务。
      LOG_ERROR("event=database_reply_failed operation_id={}",
                task.m_command.m_operationId);
    }
  }
}

void DatabaseExecutor::shutdown() {
  std::call_once(m_shutdown, [this] {
    {
      std::lock_guard lock(m_mutex);
      m_closed = true;
    }
    m_wakeup.notify_all();
    for (auto& thread : m_threads) {
      thread.join();
    }
  });
}

}  // namespace pop
