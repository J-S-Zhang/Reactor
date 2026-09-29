cmake_minimum_required(VERSION 3.10)
project(ReactorServer LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_compile_options(-Wall -Wextra -pthread)

if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
  message(WARNING "本项目面向 Linux（epoll），当前系统为 ${CMAKE_SYSTEM_NAME}")
endif()

set(REACTOR_INCLUDE ${CMAKE_CURRENT_SOURCE_DIR}/include)

set(REACTOR_SOURCES
  src/base/Timestamp.cpp
  src/base/Thread.cpp
  src/base/Logger.cpp
  src/buffer/Buffer.cpp
  src/thread/ThreadPool.cpp
  src/timer/Timer.cpp
  src/timer/TimerQueue.cpp
  src/net/InetAddress.cpp
  src/net/Socket.cpp
  src/net/Channel.cpp
  src/net/Poller.cpp
  src/net/EpollPoller.cpp
  src/net/EventLoop.cpp
  src/net/EventLoopThread.cpp
  src/net/Acceptor.cpp
  src/net/TcpConnection.cpp
  src/net/TcpServer.cpp
  src/protocol/Codec.cpp
)

add_library(reactor STATIC ${REACTOR_SOURCES})
target_include_directories(reactor PUBLIC ${REACTOR_INCLUDE})
target_link_libraries(reactor PUBLIC pthread)

add_executable(echo_server example/echo_server/main.cpp)
target_link_libraries(echo_server PRIVATE reactor)

add_executable(chat_server example/chat_server/main.cpp)
target_link_libraries(chat_server PRIVATE reactor)

enable_testing()
add_executable(Buffer_test test/Buffer_test.cpp)
target_link_libraries(Buffer_test PRIVATE reactor)
add_test(NAME Buffer_test COMMAND Buffer_test)

add_executable(ThreadPool_test test/ThreadPool_test.cpp)
target_link_libraries(ThreadPool_test PRIVATE reactor)
add_test(NAME ThreadPool_test COMMAND ThreadPool_test)

add_executable(Timer_test test/Timer_test.cpp)
target_link_libraries(Timer_test PRIVATE reactor)
add_test(NAME Timer_test COMMAND Timer_test)
