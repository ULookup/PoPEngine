# 构建 AMQP/libev 单分片样例订阅，与持久仓储在投递边界交换值对象。
find_package(amqpcpp CONFIG REQUIRED)
find_path(POP_LIBEV_INCLUDE_DIR ev.h REQUIRED)
find_library(POP_LIBEV_LIBRARY NAMES ev REQUIRED)

set(event_proto "${CMAKE_CURRENT_SOURCE_DIR}/sample_event.proto")
add_custom_command(
  OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/sample_event.pb.cc"
         "${CMAKE_CURRENT_BINARY_DIR}/sample_event.pb.h"
  COMMAND pop_protoc "-I${CMAKE_CURRENT_SOURCE_DIR}"
          "--cpp_out=${CMAKE_CURRENT_BINARY_DIR}" "${event_proto}"
  DEPENDS "${event_proto}" pop_protoc
  VERBATIM)

add_library(pop_messaging sample_messaging.cc
  "${CMAKE_CURRENT_BINARY_DIR}/sample_event.pb.cc")
target_include_directories(pop_messaging PUBLIC "${PROJECT_SOURCE_DIR}"
  PRIVATE "${CMAKE_CURRENT_BINARY_DIR}" "${POP_LIBEV_INCLUDE_DIR}")
target_link_libraries(pop_messaging PUBLIC pop_storage
  PRIVATE amqpcpp "${POP_LIBEV_LIBRARY}" pop_protobuf)
target_compile_options(pop_messaging PRIVATE -Wall -Wextra -Wpedantic)
