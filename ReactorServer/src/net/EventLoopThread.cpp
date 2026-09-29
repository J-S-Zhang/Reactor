#include "net/EventLoopThread.h"

#include "base/Logger.h"
#include "net/EventLoop.h"

namespace reactor {

EventLoopThread::EventLoopThread() : loop_(nullptr), exiting_(false) {}

/// 做什么：quit 子 loop 并 join。
/// 项目角色：Sub Reactor 线程生命周期结束。
EventLoopThread::~EventLoopThread() {
  exiting_ = true;
  if (loop_) {
    loop_->quit();
    thread_->join();
  }
}

/// 做什么：启动线程，cond 等待 loop_ 就绪后返回。
/// 项目角色：主线程获取子 EventLoop 指针以注册 TcpServer。
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

/// 做什么：栈上 EventLoop + loop() 直到 quit。
/// 项目角色：One loop per thread 模型实现。
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
