# 构建 Platform 的 Beast 监听器、Control 的 gRPC 监听器及其隔离验收客户端。
find_package(gRPC CONFIG QUIET)
if(TARGET gRPC::grpc++)
  set(control_grpc gRPC::grpc++)
else()
  find_package(PkgConfig REQUIRED)
  pkg_check_modules(CONTROL_GRPC REQUIRED IMPORTED_TARGET grpc++)
  set(control_grpc PkgConfig::CONTROL_GRPC)
endif()

add_library(pop_control_endpoints
  engine/net/platform_health.cc
  engine/net/control_endpoints.cc)
target_include_directories(pop_control_endpoints PUBLIC ${PROJECT_SOURCE_DIR})
target_link_libraries(pop_control_endpoints PRIVATE
  Boost::headers Threads::Threads ${control_grpc})
target_compile_options(pop_control_endpoints PRIVATE -Wall -Wextra -Wpedantic)

