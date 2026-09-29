#include "net/EventLoopThread.h"

#include "base/Logger.h"
#include "net/EventLoop.h"

namespace reactor {

EventLoopThread::EventLoopThread() : loop_(nullptr), exiting_(false) {}

EventLoopThread::~EventLoopThread() {
  exiting_ = true;
  if (loop_) {
    loop_->quit();
    thread_->join();
  }
}

EventLoop* EventLoopThread::startLoop() {
  thread_ = std::make_unique<Thread>([this] { threadFunc(); }, "EventLoop");
  thread_->start();
  EventLoop* loop = nullptr;
  {
    std::unique_lock<std::mutex> lock(mutex_);
    while (loop_ == nullptr) {
      cond_.wait(lock);
    }
    loop = loop_;
  }
  return loop;
}

void EventLoopThread::threadFunc() {
  EventLoop loop;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    loop_ = &loop;
    cond_.notify_one();
  }
  loop.loop();
  std::lock_guard<std::mutex> lock(mutex_);
  loop_ = nullptr;
}

}  // namespace reactor
