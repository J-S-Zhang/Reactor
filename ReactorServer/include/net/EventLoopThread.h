#pragma once

#include <condition_variable>
#include <memory>
#include <mutex>

#include "base/NonCopyable.h"
#include "base/Thread.h"

namespace reactor {

class EventLoop;

/**
 * @class EventLoopThread
 * 含义：在独立 OS 线程中运行一个 EventLoop 的生命周期管理。
 * 项目角色：方案 11.1 Main+Sub Reactor 的基础构件；主线程 accept、
 *           子线程 IO 时可「每线程一个 EventLoopThread」。
 */
class EventLoopThread : NonCopyable {
 public:
  EventLoopThread();
  ~EventLoopThread();

  /// 含义：启动线程并返回子线程 EventLoop*。角色：调用方在子 loop 上注册服务。
  EventLoop* startLoop();

 private:
  /// 含义：子线程体：构造 EventLoop、loop()。角色：Thread 入口。
  void threadFunc();

  EventLoop* loop_;                  ///< 含义：指向子线程栈上 EventLoop。角色：startLoop 返回。
  bool exiting_;                     ///< 含义：析构标志。角色：预留扩展。
  std::unique_ptr<Thread> thread_;   ///< 含义：OS 线程。角色：承载 threadFunc。
  std::mutex mutex_;                 ///< 含义：保护 loop_ 就绪同步。角色：cond_ 配合。
  std::condition_variable cond_;     ///< 含义：loop_ 非空通知。角色：startLoop 等待就绪。
};

}  // namespace reactor
