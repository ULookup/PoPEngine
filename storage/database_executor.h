// 将固定大小样例数据库任务送至专用线程，并在释放连接后回投值类型结果。

#pragma once

#include <condition_variable>
#include <cstddef>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "storage/sample_repository.h"

namespace pop {

// 调用方拥有的投递目标，只负责发送完成消息，外部状态不计入池自有预算。
using DatabaseCompletion =
    std::shared_ptr<const std::function<void(SampleResult)>>;

// 独立于 Scene 的有界执行池；持有方须让分片 Repository 和结果接收端存活到排空。
class DatabaseExecutor {
 public:
  // 设置线程数与排队条数、字节上限，字节核算包含命令和结果目标句柄。
  DatabaseExecutor(SampleRepository& repository, std::size_t threads,
                   std::size_t max_entries, std::size_t max_bytes);
  // 停止接收，等待已受理的任务完成回投。
  ~DatabaseExecutor();

  // 入队即返回；满载、目标无效、截止或已关闭均返回 false。
  bool postApply(SampleCommand command, TimePoint deadline,
                 DatabaseCompletion completion);
  // 向主写端查询稳定操作号，受理语义与写任务相同。
  bool postQuery(std::uint64_t operation_id, TimePoint deadline,
                 DatabaseCompletion completion);
  // 持有方在池线程以外调用，关闭后允许重复等待。
  void shutdown();

 private:
  // 数据库任务的固定操作类型。
  enum class Operation {
    Apply,  // 提交样例命令。
    Query,  // 查询稳定操作标识。
  };

  // 同一次数据库执行需要的全部池自有数据。
  struct Task {
    Operation m_operation;            // 写入或查询。
    SampleCommand m_command;          // 查询时只使用原操作标识。
    TimePoint m_deadline;             // 单调时钟上的最晚受理时间。
    DatabaseCompletion m_completion;  // 外部接收端的存活句柄。
  };

  // 在锁内检查关闭和容量边界，成功后唤醒一个数据库线程。
  bool post(Task task);
  // 在专用线程逐项取出命令，持锁期间不访问数据库。
  void run();

  SampleRepository& m_repository;      // 由服务持有，寿命长于池。
  const std::size_t m_queueLimit;      // 条数和字节两种上限折算后的排队条数。
  std::mutex m_mutex;                  // 保护队列和关闭标记。
  std::condition_variable m_wakeup;    // 入队与关闭时唤醒等待者。
  std::deque<Task> m_queue;            // 尚未开始的命令。
  bool m_closed = false;               // 置位后拒绝新命令并排空存量。
  std::vector<std::thread> m_threads;  // 专用数据库执行线程。
  std::once_flag m_shutdown;           // 并发关闭时仅一方回收线程。
};

}  // namespace pop
