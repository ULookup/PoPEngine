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
