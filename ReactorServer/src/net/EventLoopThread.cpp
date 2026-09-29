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

/// 启动线程并阻塞直到子线程 EventLoop 创建完成
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

/// 子线程：构造 EventLoop 并 loop()，退出后清空 loop_
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
