# 生成样例操作的 ODB 映射，与同连接的参数化 SQL 和数据库执行池共同编译。

find_program(POP_ODB_EXECUTABLE odb REQUIRED)
find_path(POP_ODB_INCLUDE_DIR odb/core.hxx REQUIRED)
find_library(POP_ODB_LIBRARY odb REQUIRED)
find_library(POP_ODB_MYSQL_LIBRARY odb-mysql REQUIRED)
find_path(POP_MYSQL_INCLUDE_DIR mysql.h PATH_SUFFIXES mysql REQUIRED)
find_library(POP_MYSQL_LIBRARY mysqlclient REQUIRED)

set(operation_mapping "${CMAKE_CURRENT_BINARY_DIR}/sample_operation-odb")
add_custom_command(
  OUTPUT "${operation_mapping}.hxx" "${operation_mapping}.ixx"
         "${operation_mapping}.cxx"
  COMMAND "${POP_ODB_EXECUTABLE}" --std c++11 --database mysql
          "-I${POP_ODB_INCLUDE_DIR}" --output-dir "${CMAKE_CURRENT_BINARY_DIR}"
          "${CMAKE_CURRENT_SOURCE_DIR}/sample_operation.h"
  DEPENDS sample_operation.h VERBATIM)

add_library(pop_storage sample_repository.cc database_executor.cc
  "${operation_mapping}.cxx")
target_include_directories(pop_storage PUBLIC "${PROJECT_SOURCE_DIR}"
  PRIVATE "${CMAKE_CURRENT_BINARY_DIR}" "${CMAKE_CURRENT_SOURCE_DIR}"
  "${POP_ODB_INCLUDE_DIR}" "${POP_MYSQL_INCLUDE_DIR}")
target_link_libraries(pop_storage PRIVATE "${POP_ODB_MYSQL_LIBRARY}"
  "${POP_ODB_LIBRARY}" "${POP_MYSQL_LIBRARY}" Threads::Threads spdlog::spdlog)
