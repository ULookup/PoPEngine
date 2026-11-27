# 提供 MsQuic::MsQuic，供 Gateway 和协议驱动共用同一 Linux SDK。
# 优先使用共享库，以隔离 QUIC 专用 TLS 后端与进程内 Asio 的 OpenSSL。
find_path(MsQuic_INCLUDE_DIR msquic.h HINTS
  "${MsQuic_ROOT}" "$ENV{MsQuic_ROOT}" "${PROJECT_SOURCE_DIR}/build/deps/msquic"
  PATH_SUFFIXES include)
find_library(MsQuic_LIBRARY NAMES msquic HINTS
  "${MsQuic_ROOT}" "$ENV{MsQuic_ROOT}" "${PROJECT_SOURCE_DIR}/build/deps/msquic"
  PATH_SUFFIXES lib)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(MsQuic REQUIRED_VARS MsQuic_LIBRARY MsQuic_INCLUDE_DIR)
if(MsQuic_FOUND AND NOT TARGET MsQuic::MsQuic)
  add_library(MsQuic::MsQuic UNKNOWN IMPORTED)
  set_target_properties(MsQuic::MsQuic PROPERTIES
    IMPORTED_LOCATION "${MsQuic_LIBRARY}"
    INTERFACE_INCLUDE_DIRECTORIES "${MsQuic_INCLUDE_DIR}")
  if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    # MsQuic 共享库依赖发行版的 libcrypto。隐藏应用静态链接的 OpenSSL
    # 符号，确保两套库各自使用配套的分配器、证书对象及提供程序。
    target_link_options(MsQuic::MsQuic INTERFACE
      "LINKER:--exclude-libs,libcrypto.a:libssl.a")
  endif()
endif()
