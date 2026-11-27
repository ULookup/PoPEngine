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

if(BUILD_TESTING)
  if(TARGET gRPC::grpc_cpp_plugin)
    set(control_plugin "$<TARGET_FILE:gRPC::grpc_cpp_plugin>")
  else()
    find_program(control_plugin grpc_cpp_plugin REQUIRED)
  endif()

  set(control_proto_dir "${CMAKE_CURRENT_BINARY_DIR}/control_proto")
  add_custom_command(
    OUTPUT "${control_proto_dir}/control.pb.cc"
           "${control_proto_dir}/control.pb.h"
           "${control_proto_dir}/control.grpc.pb.cc"
           "${control_proto_dir}/control.grpc.pb.h"
    COMMAND ${CMAKE_COMMAND} -E make_directory "${control_proto_dir}"
    COMMAND $<TARGET_FILE:pop_protoc> "-I${PROJECT_SOURCE_DIR}/proto"
            "--cpp_out=${control_proto_dir}" "--grpc_out=${control_proto_dir}"
            "--plugin=protoc-gen-grpc=${control_plugin}"
            "${PROJECT_SOURCE_DIR}/proto/control.proto"
    DEPENDS "${PROJECT_SOURCE_DIR}/proto/control.proto" pop_protoc
    VERBATIM)
  add_executable(control_driver tests/control_driver.cc
    "${control_proto_dir}/control.pb.cc"
    "${control_proto_dir}/control.grpc.pb.cc")
  target_include_directories(control_driver PRIVATE "${control_proto_dir}")
  target_link_libraries(control_driver PRIVATE pop_protobuf ${control_grpc})

  add_test(NAME ControlRpcDeadline COMMAND Python3::Interpreter -B
    "${PROJECT_SOURCE_DIR}/tests/control_integration.py" rpc
    $<TARGET_FILE:control> $<TARGET_FILE:control_driver>)
  set_tests_properties(ControlRpcDeadline PROPERTIES TIMEOUT 25)

  find_program(POP_NGINX_EXECUTABLE nginx
    HINTS "${PROJECT_SOURCE_DIR}/build/phase2-deps/nginx/usr/sbin" /usr/sbin)
  add_test(NAME PlatformHttpsHealth COMMAND Python3::Interpreter -B
    "${PROJECT_SOURCE_DIR}/tests/control_integration.py" http
    $<TARGET_FILE:platform> "${POP_NGINX_EXECUTABLE}"
    "${PROJECT_SOURCE_DIR}/deploy/nginx/platform.conf.in")
  set_tests_properties(PlatformHttpsHealth PROPERTIES
    TIMEOUT 20 SKIP_RETURN_CODE 77)
endif()
