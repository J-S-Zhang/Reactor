cmake_minimum_required(VERSION 3.10)
project(ReactorTast LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
add_compile_options(-Wall -Wextra -pthread)

if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
  message(WARNING "压测客户端建议在 Linux 下编译运行")
endif()

set(REACTOR_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/../ReactorServer)
add_subdirectory(${REACTOR_ROOT} ${CMAKE_BINARY_DIR}/reactor EXCLUDE_FROM_ALL)

add_executable(bench_echo src/bench_echo.cpp)
target_link_libraries(bench_echo PRIVATE reactor pthread)

add_executable(bench_connect src/bench_connect.cpp)
target_link_libraries(bench_connect PRIVATE reactor pthread)

add_executable(bench_long_conn src/bench_long_conn.cpp)
target_link_libraries(bench_long_conn PRIVATE reactor pthread)
