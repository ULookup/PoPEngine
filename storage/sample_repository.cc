// 在显式 ODB 事务中读写样例计数。原生预处理语句借用同一事务的实际连接。

#include "storage/sample_repository.h"

#include <mysql.h>

#include <array>
#include <initializer_list>
#include <limits>
#include <odb/mysql/connection-factory.hxx>
#include <odb/mysql/database.hxx>
#include <odb/mysql/transaction.hxx>
#include <stdexcept>
#include <string_view>

#include "sample_operation-odb.hxx"

namespace pop {
namespace {

using Number = std::uint64_t;  // 样例表的无符号 BIGINT 与绑定参数。
using Params = std::initializer_list<Number>;  // 本次执行期间借用的参数。
using Transaction = odb::mysql::transaction;   // 所有仓储共用的事务上下文。

// 连接池在物理连接创建时安装有界网络超时和 InnoDB 会话级隔离策略。
class ShardConnectionPool final : public odb::mysql::connection_pool_factory {
 public:
  // 限制同时借出的主写连接；所有超时均以秒计。
  explicit ShardConnectionPool(std::size_t connections)
      : connection_pool_factory(connections) {}

 private:
  // 只在 ODB 需要新连接时设置选项，连接验证和归还仍由 ODB 负责。
  pooled_connection_ptr create() override {
    std::unique_ptr<MYSQL, decltype(&mysql_close)> handle(mysql_init(nullptr),
                                                          mysql_close);
    if (!handle) {
      throw std::bad_alloc();
    }
    constexpr unsigned CONNECT_TIMEOUT_SECONDS = 5;
    constexpr unsigned IO_TIMEOUT_SECONDS = 15;
    auto& config = connection_factory::database();
    if (mysql_options(handle.get(), MYSQL_OPT_CONNECT_TIMEOUT,
                      &CONNECT_TIMEOUT_SECONDS) ||
        mysql_options(handle.get(), MYSQL_OPT_READ_TIMEOUT,
                      &IO_TIMEOUT_SECONDS) ||
        mysql_options(handle.get(), MYSQL_OPT_WRITE_TIMEOUT,
                      &IO_TIMEOUT_SECONDS) ||
        mysql_options(handle.get(), MYSQL_SET_CHARSET_NAME, config.charset()) ||
        mysql_options(handle.get(), MYSQL_INIT_COMMAND,
                      "SET SESSION transaction_isolation='READ-COMMITTED',"
                      "innodb_lock_wait_timeout=10,lock_wait_timeout=10")) {
      throw std::runtime_error("invalid MySQL connection options");
    }
    // ODB 根据匹配行数判断对象是否存在，必须保留 CLIENT_FOUND_ROWS。
    if (!mysql_real_connect(handle.get(), config.host(), config.user(),
                            config.password(), config.db(), config.port(),
                            config.socket(),
                            config.client_flags() | CLIENT_FOUND_ROWS)) {
      throw std::runtime_error("MySQL connection error " +
                               std::to_string(mysql_errno(handle.get())));
    }
    return pooled_connection_ptr(
        new pooled_connection(*this, handle.release()));
  }
};

// 只绑定无符号整数，样例 SQL 中所有业务数据都走参数绑定。
MYSQL_BIND BindNumber(const Number* value) {
  MYSQL_BIND binding{};
  binding.buffer_type = MYSQL_TYPE_LONGLONG;
  binding.buffer = const_cast<Number*>(value);
  binding.is_unsigned = true;
  return binding;
}

// RAII 语句，生命周期限于显式传入的事务。
class Statement {
 public:
  // 在事务连接上准备固定 SQL，并立即用所有实参执行。
  Statement(Transaction& transaction, std::string_view sql, Params params)
      : m_handle(mysql_stmt_init(transaction.connection().handle()),
                 mysql_stmt_close) {
    if (!m_handle) {
      throw std::runtime_error("mysql_stmt_init failed");
    }
    check(mysql_stmt_prepare(m_handle.get(), sql.data(), sql.size()));
    std::vector<MYSQL_BIND> bindings;
    bindings.reserve(params.size());
    for (const Number& param : params) {
      bindings.push_back(BindNumber(&param));
    }
    if (mysql_stmt_param_count(m_handle.get()) != bindings.size()) {
      throw std::logic_error("incorrect MySQL parameter count");
    }
    if (!bindings.empty()) {
      check(mysql_stmt_bind_param(m_handle.get(), bindings.data()));
    }
    check(mysql_stmt_execute(m_handle.get()));
  }

  // 取得写入的实际影响行数，用于判定认领写操作是否成功。
  Number affected() const { return mysql_stmt_affected_rows(m_handle.get()); }

  // 读取固定列数的无符号整数结果，查询 SQL 自行约束行数。
  template <std::size_t Columns>
  std::vector<std::array<Number, Columns>> rows() {
    if (mysql_stmt_field_count(m_handle.get()) != Columns) {
      throw std::logic_error("incorrect MySQL column count");
    }
    std::array<Number, Columns> row{};
    std::array<MYSQL_BIND, Columns> bindings{};
    for (std::size_t i = 0; i < Columns; ++i) {
      bindings[i] = BindNumber(&row[i]);
    }
    check(mysql_stmt_bind_result(m_handle.get(), bindings.data()));
    std::vector<std::array<Number, Columns>> result;
    int status;
    while ((status = mysql_stmt_fetch(m_handle.get())) != MYSQL_NO_DATA) {
      check(status);
      result.push_back(row);
    }
    return result;
  }

 private:
  // 驱动错误保留数值错误码，凭据与业务参数不进入异常文本。
  void check(int result) const {
    if (result != 0) {
      throw std::runtime_error(
          "MySQL statement error " +
          std::to_string(mysql_stmt_errno(m_handle.get())));
    }
  }

  // 独占语句句柄，析构后事务连接仍由调用方持有。
  std::unique_ptr<MYSQL_STMT, decltype(&mysql_stmt_close)> m_handle;
};

// 将持久操作行转换为本样例唯一事件的完整信封数据。
SampleEvent Event(const SampleOperation& row) {
  return {.m_eventId = row.m_operationId,
          .m_operationId = row.m_operationId,
          .m_aggregateId = row.m_aggregateId,
          .m_version = row.m_version,
          .m_value = row.m_value,
          .m_committedAtMs = row.m_committedAtMs};
}

// 在已有事务中串行化同一操作号、锁定计数器并一次写入操作结果与 Outbox。
SampleResult Apply(Transaction& transaction, SampleCommand command,
                   bool& created) {
  Statement(transaction,
            "INSERT INTO sample_operation(operation_id,aggregate_id,delta) "
            "VALUES (?,?,?) ON DUPLICATE KEY UPDATE operation_id=operation_id",
            {command.m_operationId, command.m_aggregateId, command.m_delta});

  SampleOperation row{};
  transaction.database().load(command.m_operationId, row);
  if (row.m_delta != command.m_delta ||
      row.m_aggregateId != command.m_aggregateId) {
    return {SampleStatus::Rejected, {}};
  }
  if (row.m_version != 0) {
    return {SampleStatus::Committed, Event(row)};
  }

  Statement(transaction,
            "INSERT INTO sample_counter(aggregate_id) VALUES(?) "
            "ON DUPLICATE KEY UPDATE aggregate_id=aggregate_id",
            {command.m_aggregateId});
  const auto current =
      Statement(transaction,
                "SELECT value,version,CAST(UNIX_TIMESTAMP(NOW(3))*1000 "
                "AS UNSIGNED) FROM sample_counter WHERE aggregate_id=? "
                "FOR UPDATE",
                {command.m_aggregateId})
          .rows<3>()
          .at(0);
  constexpr Number MAX_VALUE = std::numeric_limits<Number>::max();
  if (command.m_delta > MAX_VALUE - current[0] || current[1] == MAX_VALUE) {
    return {SampleStatus::Rejected, {}};
  }
  row.m_value = current[0] + command.m_delta;
  row.m_version = current[1] + 1;
  row.m_committedAtMs = current[2];
  Statement(transaction,
            "UPDATE sample_counter SET value=?,version=? WHERE aggregate_id=?",
            {row.m_value, row.m_version, command.m_aggregateId});
  transaction.database().update(row);
  Statement(transaction,
            "INSERT INTO sample_outbox(event_id,available_at) "
            "VALUES(?,UTC_TIMESTAMP(3))",
            {row.m_operationId});
  created = true;
  return {SampleStatus::Committed, Event(row)};
}

// 按同一列序读取已提交的、包含绝对值的事件页。
std::vector<SampleEvent> ReadEvents(Transaction& transaction,
                                    std::string_view sql, Params params) {
  auto rows = Statement(transaction, sql, params).rows<5>();
  std::vector<SampleEvent> events;
  events.reserve(rows.size());
  for (const auto& [operation, aggregate, version, value, timestamp] : rows) {
    events.push_back({.m_eventId = operation,
                      .m_operationId = operation,
                      .m_aggregateId = aggregate,
                      .m_version = version,
                      .m_value = value,
                      .m_committedAtMs = timestamp});
  }
  return events;
}

}  // namespace

SampleRepository::SampleRepository(const SampleDatabaseConfig& config) {
  if (config.m_connections == 0 || config.m_user.empty() ||
      config.m_database.empty() || config.m_host.empty() ||
      config.m_port == 0 || config.m_port > 65535) {
    throw std::invalid_argument("invalid MySQL shard configuration");
  }
  auto factory = std::make_unique<ShardConnectionPool>(config.m_connections);
  m_database = std::make_unique<odb::mysql::database>(
      config.m_user, config.m_password, config.m_database, config.m_host,
      config.m_port, nullptr, "utf8mb4", 0, std::move(factory));
}

SampleRepository::~SampleRepository() = default;

SampleResult SampleRepository::apply(SampleCommand command,
                                     TimePoint deadline) {
  if (!command.m_operationId || !command.m_aggregateId || !command.m_delta ||
      Clock::now() >= deadline) {
    return {SampleStatus::Rejected, {}};
  }
  bool committing = false;
  try {
    Transaction transaction(m_database->begin());
    bool created = false;
    auto result = Apply(transaction, command, created);
    if (!created) {
      // 原结果已经提交；冲突或超限则回滚本次占位记录。
      return result;
    }
    if (Clock::now() >= deadline) {
      return {SampleStatus::Rejected, {}};  // 提交尚未发起，析构回滚。
    }
    committing = true;
    transaction.commit();
    return result;
  } catch (const std::exception&) {
    return {committing ? SampleStatus::Unknown : SampleStatus::Unavailable, {}};
  }
}

SampleResult SampleRepository::query(Number operation_id) {
  if (!operation_id) {
    return {SampleStatus::Rejected, {}};
  }
  try {
    Transaction transaction(m_database->begin());
    SampleOperation row{};
    bool found = m_database->find(operation_id, row);
    transaction.commit();
    return found ? SampleResult{SampleStatus::Committed, Event(row)}
                 : SampleResult{SampleStatus::Missing, {}};
  } catch (const std::exception&) {
    return {SampleStatus::Unavailable, {}};
  }
}

std::optional<OutboxClaim> SampleRepository::claimOutbox(Milliseconds lease) {
  if (lease.count() <= 0) {
    throw std::invalid_argument("expected positive Outbox lease");
  }
  Transaction transaction(m_database->begin());
  auto rows = Statement(transaction,
                        "SELECT event_id,generation,attempt FROM sample_outbox "
                        "WHERE state=0 AND available_at<=UTC_TIMESTAMP(3) "
                        "ORDER BY available_at,event_id LIMIT 1 "
                        "FOR UPDATE SKIP LOCKED",
                        {})
                  .rows<3>();
  if (rows.empty()) {
    transaction.commit();
    return std::nullopt;
  }
  const auto& [id, generation, attempt] = rows.front();
  Statement(
      transaction,
      "UPDATE sample_outbox SET generation=generation+1,attempt=attempt+1,"
      "available_at=TIMESTAMPADD(MICROSECOND,?*1000,UTC_TIMESTAMP(3)) "
      "WHERE event_id=?",
      {static_cast<Number>(lease.count()), id});
  SampleOperation row{};
  m_database->load(id, row);
  transaction.commit();
  return OutboxClaim{.m_event = Event(row),
                     .m_generation = generation + 1,
                     .m_attempt = attempt + 1};
}

bool SampleRepository::markPublished(const OutboxClaim& claim) {
  Transaction transaction(m_database->begin());
  auto affected = Statement(transaction,
                            "UPDATE sample_outbox SET state=1 WHERE event_id=? "
                            "AND generation=? AND state=0 AND "
                            "available_at>UTC_TIMESTAMP(3)",
                            {claim.m_event.m_eventId, claim.m_generation})
                      .affected();
  transaction.commit();
  return affected == 1;
}

bool SampleRepository::deferOutbox(const OutboxClaim& claim, Milliseconds delay,
                                   bool quarantine) {
  if (delay.count() < 0) {
    throw std::invalid_argument("expected nonnegative Outbox retry delay");
  }
  Transaction transaction(m_database->begin());
  auto affected =
      Statement(
          transaction,
          "UPDATE sample_outbox SET state=?,generation=generation+1,"
          "available_at=TIMESTAMPADD(MICROSECOND,?*1000,UTC_TIMESTAMP(3)) "
          "WHERE event_id=? AND generation=? AND state=0 AND "
          "available_at>UTC_TIMESTAMP(3)",
          {quarantine ? 2u : 0u, static_cast<Number>(delay.count()),
           claim.m_event.m_eventId, claim.m_generation})
          .affected();
  transaction.commit();
  return affected == 1;
}

bool SampleRepository::replayOutbox(Number event_id) {
  Transaction transaction(m_database->begin());
  auto affected = Statement(transaction,
                            "UPDATE sample_outbox SET state=0,attempt=0,"
                            "available_at=UTC_TIMESTAMP(3) "
                            "WHERE event_id=? AND state=2",
                            {event_id})
                      .affected();
  transaction.commit();
  return affected == 1;
}

bool SampleRepository::applyInbox(const SampleEvent& event) {
  if (!event.m_eventId || event.m_eventId != event.m_operationId ||
      !event.m_aggregateId || !event.m_version || !event.m_value ||
      !event.m_committedAtMs) {
    throw std::invalid_argument("invalid sample event");
  }
  Transaction transaction(m_database->begin());
  Statement(transaction,
            "INSERT INTO sample_inbox(event_id,aggregate_id,version,value,"
            "committed_at_ms) VALUES(?,?,?,?,?) "
            "ON DUPLICATE KEY UPDATE event_id=event_id",
            {event.m_eventId, event.m_aggregateId, event.m_version,
             event.m_value, event.m_committedAtMs});
  const auto previous =
      Statement(transaction,
                "SELECT aggregate_id,version,value,committed_at_ms,applied "
                "FROM sample_inbox WHERE event_id=?",
                {event.m_eventId})
          .rows<5>()
          .at(0);
  if (previous[0] != event.m_aggregateId || previous[1] != event.m_version ||
      previous[2] != event.m_value || previous[3] != event.m_committedAtMs) {
    throw std::invalid_argument("conflicting event identity");
  }
  if (previous[4]) {
    transaction.commit();
    return false;
  }

  Statement(transaction,
            "INSERT INTO sample_projection(aggregate_id) VALUES(?) "
            "ON DUPLICATE KEY UPDATE aggregate_id=aggregate_id",
            {event.m_aggregateId});
  // 绝对值允许乱序；先比较旧版本更新值，再发布版本和实际消费次数。
  Statement(
      transaction,
      "UPDATE sample_projection SET value=IF(version<?,?,value),"
      "version=GREATEST(version,?),applications=applications+1 "
      "WHERE aggregate_id=?",
      {event.m_version, event.m_value, event.m_version, event.m_aggregateId});
  Statement(transaction, "UPDATE sample_inbox SET applied=1 WHERE event_id=?",
            {event.m_eventId});
  transaction.commit();
  return true;
}

Number SampleRepository::projectionApplications(Number aggregate_id) {
  Transaction transaction(m_database->begin());
  auto rows = Statement(transaction,
                        "SELECT applications FROM sample_projection "
                        "WHERE aggregate_id=?",
                        {aggregate_id})
                  .rows<1>();
  transaction.commit();
  return rows.empty() ? 0 : rows.front()[0];
}

std::optional<SampleEvent> SampleRepository::snapshot(Number aggregate_id) {
  Transaction transaction(m_database->begin());
  auto events = ReadEvents(transaction,
                           "SELECT operation_id,aggregate_id,version,value,"
                           "committed_at_ms FROM sample_operation "
                           "WHERE aggregate_id=? ORDER BY version DESC LIMIT 1",
                           {aggregate_id});
  transaction.commit();
  if (events.empty()) {
    return std::nullopt;
  }
  return events.front();
}

std::vector<SampleEvent> SampleRepository::eventsAfter(Number aggregate_id,
                                                       Number after_version,
                                                       Number through_version,
                                                       std::size_t limit) {
  Transaction transaction(m_database->begin());
  auto events =
      ReadEvents(transaction,
                 "SELECT operation_id,aggregate_id,version,value,"
                 "committed_at_ms FROM sample_operation "
                 "WHERE aggregate_id=? AND version>? AND version<=? "
                 "ORDER BY version LIMIT ?",
                 {aggregate_id, after_version, through_version, limit});
  transaction.commit();
  return events;
}

}  // namespace pop
