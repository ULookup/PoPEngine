// 定义由 ODB 生成映射的已提交操作；Schema 与隔离样例 DDL 对应。

#pragma once

#include <odb/core.hxx>

namespace pop {

// 重试身份、请求参数与第一次成功后的不可变业务结果。
#pragma db object table("sample_operation")
struct SampleOperation {
#pragma db id column("operation_id")
  unsigned long long m_operationId;  // 提交前持久保存的稳定操作号。
#pragma db column("aggregate_id")
  unsigned long long m_aggregateId;  // 原命令指定的聚合。
#pragma db column("delta")
  unsigned long long m_delta;  // 原命令指定的增量，供重试核对。
#pragma db column("version")
  unsigned long long m_version;  // 聚合版本，零仅存在于创建事务内。
#pragma db column("value")
  unsigned long long m_value;  // 此版本的完整计数。
#pragma db column("committed_at_ms")
  unsigned long long m_committedAtMs;  // 事务内数据库采样的 Unix 毫秒。
};

}  // namespace pop
