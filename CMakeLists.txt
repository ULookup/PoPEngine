# 构建服务端运行库和四个进程入口，并向 CTest 注册本地验收用例。

cmake_minimum_required(VERSION 3.22)
project(PopEngine LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
find_package(Threads REQUIRED)
find_package(yaml-cpp CONFIG REQUIRED)
find_package(spdlog CONFIG REQUIRED)
find_package(Lua REQUIRED)

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

add_library(pop_apps apps/process.cc)
target_link_libraries(pop_apps PUBLIC pop_runtime)

foreach(role IN ITEMS gateway game_worker platform control)
  add_executable(${role} apps/${role}/main.cc)
  target_link_libraries(${role} PRIVATE pop_apps)
endforeach()

include(CTest)
if(BUILD_TESTING)
  find_package(Python3 REQUIRED COMPONENTS Interpreter)
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
