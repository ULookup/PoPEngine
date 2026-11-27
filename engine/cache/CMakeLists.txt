# 将样例计数器读视图链接到存储库、redis-plus-plus 和 hiredis。

find_path(REDIS_PLUS_PLUS_INCLUDE_DIR sw/redis++/redis++.h REQUIRED)
find_library(REDIS_PLUS_PLUS_LIBRARY NAMES redis++ REQUIRED)
find_path(HIREDIS_INCLUDE_DIR hiredis/hiredis.h REQUIRED)
find_library(HIREDIS_LIBRARY NAMES hiredis REQUIRED)

add_library(pop_cache sample_read_view.cc)
target_include_directories(pop_cache PUBLIC "${PROJECT_SOURCE_DIR}"
  PRIVATE "${REDIS_PLUS_PLUS_INCLUDE_DIR}" "${HIREDIS_INCLUDE_DIR}")
target_link_libraries(pop_cache PUBLIC pop_storage
  PRIVATE "${REDIS_PLUS_PLUS_LIBRARY}" "${HIREDIS_LIBRARY}")
target_compile_options(pop_cache PRIVATE -Wall -Wextra -Wpedantic)
