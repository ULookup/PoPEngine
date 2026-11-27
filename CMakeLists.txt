# 构建服务端运行库和四个进程入口，并向 CTest 注册本地验收用例。

cmake_minimum_required(VERSION 3.22)
project(PopEngine LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
list(APPEND CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake")
include(CTest)
if(BUILD_TESTING)
  find_package(Python3 REQUIRED COMPONENTS Interpreter)
endif()
find_package(Threads REQUIRED)
find_package(yaml-cpp CONFIG REQUIRED)
find_package(spdlog CONFIG REQUIRED)
find_package(Lua REQUIRED)
find_package(Boost REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(MsQuic REQUIRED)
find_package(Protobuf REQUIRED)
add_executable(pop_protoc ALIAS protobuf::protoc)
add_library(pop_protobuf ALIAS protobuf::libprotobuf)
include(cmake/ControlEndpoints.cmake)

set(realtime_proto "${PROJECT_SOURCE_DIR}/proto/realtime.proto")
add_custom_command(
  OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/realtime.pb.cc"
         "${CMAKE_CURRENT_BINARY_DIR}/realtime.pb.h"
  COMMAND pop_protoc "-I${PROJECT_SOURCE_DIR}/proto"
          "--cpp_out=${CMAKE_CURRENT_BINARY_DIR}" "${realtime_proto}"
  DEPENDS "${realtime_proto}" pop_protoc
  VERBATIM)

add_library(pop_runtime
  engine/core/config.cc
  engine/core/log.cc
  engine/runtime/mailbox.cc
  engine/runtime/execution_shard.cc
  engine/script/lua_vm.cc
  engine/scene/scene_shard.cc
  services/game_worker.cc)
target_include_directories(pop_runtime PUBLIC ${PROJECT_SOURCE_DIR} PRIVATE ${LUA_INCLUDE_DIR})
target_link_libraries(pop_runtime PUBLIC Threads::Threads spdlog::spdlog
  PRIVATE yaml-cpp::yaml-cpp ${LUA_LIBRARIES})
target_compile_options(pop_runtime PRIVATE -Wall -Wextra -Wpedantic)

add_library(pop_net
  engine/net/send_queue.cc
  engine/net/tls_transport.cc
  engine/net/quic_transport.cc
  engine/net/wire_frame.cc
  engine/net/pending_replies.cc
  engine/net/scene_ingress.cc
  engine/net/input_forwarder.cc
  engine/net/state_channel.cc
  engine/net/realtime_gateway.cc
  "${CMAKE_CURRENT_BINARY_DIR}/realtime.pb.cc")
target_include_directories(pop_net PRIVATE "${CMAKE_CURRENT_BINARY_DIR}")
target_link_libraries(pop_net PUBLIC pop_runtime PRIVATE Boost::headers
  OpenSSL::SSL OpenSSL::Crypto MsQuic::MsQuic pop_protobuf)
target_compile_options(pop_net PRIVATE -Wall -Wextra -Wpedantic)

add_library(pop_apps apps/process.cc)
target_link_libraries(pop_apps PUBLIC pop_net pop_control_endpoints)

foreach(role IN ITEMS gateway game_worker platform control)
  add_executable(${role} apps/${role}/main.cc)
  target_link_libraries(${role} PRIVATE pop_apps)
endforeach()

option(POP_BUILD_DATA "构建阶段三持久化、可靠消息与缓存模块" OFF)
if(POP_BUILD_DATA)
  add_subdirectory(storage)
  add_subdirectory(messaging)
endif()

if(BUILD_TESTING)
  add_executable(quic_driver tests/quic_driver.cc)
  target_include_directories(quic_driver PRIVATE ${PROJECT_SOURCE_DIR})
  target_link_libraries(quic_driver PRIVATE MsQuic::MsQuic OpenSSL::Crypto Threads::Threads)
  add_executable(transport_queue_test tests/transport_queue_test.cc)
  target_link_libraries(transport_queue_test PRIVATE pop_net OpenSSL::SSL)
  add_test(NAME TransportQueue COMMAND transport_queue_test)
  add_executable(input_forwarder_test tests/input_forwarder_test.cc)
  target_include_directories(input_forwarder_test PRIVATE "${CMAKE_CURRENT_BINARY_DIR}")
  target_link_libraries(input_forwarder_test PRIVATE pop_net pop_protobuf)
  add_test(NAME InputBatching COMMAND input_forwarder_test)
  add_test(NAME RealtimeIngress COMMAND Python3::Interpreter -B
    "${PROJECT_SOURCE_DIR}/tests/realtime_integration.py"
    $<TARGET_FILE:game_worker> $<TARGET_FILE:gateway> $<TARGET_FILE:quic_driver>
    $<TARGET_FILE:transport_queue_test> $<TARGET_FILE:pop_protoc>)
  set_tests_properties(RealtimeIngress PROPERTIES TIMEOUT 45)
  add_executable(runtime_test tests/runtime_test.cc)
  target_link_libraries(runtime_test PRIVATE pop_runtime)
  foreach(scenario IN ITEMS TwoSpaces WorkResults MailboxLimits WorkerIngress)
    add_test(NAME ${scenario} COMMAND runtime_test ${scenario})
    set_tests_properties(${scenario} PROPERTIES TIMEOUT 10)
  endforeach()
  foreach(scenario IN ITEMS ProcessLifecycle UnassignedWorker InvalidConfig)
    add_test(NAME ${scenario} COMMAND ${Python3_EXECUTABLE}
      ${PROJECT_SOURCE_DIR}/tests/process_lifecycle.py ${scenario}
      $<TARGET_FILE_DIR:game_worker> ${PROJECT_SOURCE_DIR}/configs)
    set_tests_properties(${scenario} PROPERTIES TIMEOUT 25)
  endforeach()
endif()
