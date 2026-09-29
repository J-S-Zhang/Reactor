#pragma once

#include <condition_variable>
#include <memory>
#include <mutex>

#include "base/NonCopyable.h"
#include "base/Thread.h"

namespace reactor {

class EventLoop;

class EventLoopThread : NonCopyable {
 public:
  EventLoopThread();
  ~EventLoopThread();

  EventLoop* startLoop();

 private:
  void threadFunc();

  EventLoop* loop_;
  bool exiting_;
  std::unique_ptr<Thread> thread_;
  std::mutex mutex_;
  std::condition_variable cond_;
};

}  // namespace reactor
