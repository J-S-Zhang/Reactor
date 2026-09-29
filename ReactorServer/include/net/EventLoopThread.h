#pragma once

#include <condition_variable>
#include <memory>
#include <mutex>

#include "base/NonCopyable.h"
#include "base/Thread.h"

namespace reactor {

class EventLoop;

/// 在独立线程中运行一个 EventLoop
class EventLoopThread : NonCopyable {
 public:
  EventLoopThread();
  ~EventLoopThread();

  EventLoop* startLoop();

 private:
  void threadFunc();

  EventLoop* loop_;                  ///< 子线程上的 EventLoop 指针（栈对象地址）
  bool exiting_;                     ///< 析构时置 true
  std::unique_ptr<Thread> thread_;   ///< 承载 threadFunc 的线程
  std::mutex mutex_;                 ///< 与 cond_ 配合等待 loop_ 就绪
  std::condition_variable cond_;
};

}  // namespace reactor
