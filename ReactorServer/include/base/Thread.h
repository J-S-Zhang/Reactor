#pragma once

#include <functional>
#include <memory>
#include <pthread.h>
#include <sys/types.h>
#include <unistd.h>

#include "base/NonCopyable.h"

namespace reactor {

/**
 * @class Thread
 * 含义：对 POSIX 线程的一次封装（函数 + 可选名字）。
 * 项目角色：为 ThreadPool 工作线程、AsyncLogger 后台线程、EventLoopThread
 *           提供统一的创建/join 接口，隐藏 pthread 细节。
 */
class Thread : NonCopyable {
 public:
  using ThreadFunc = std::function<void()>;

  /// 含义：保存入口函数与线程名，不启动。角色：ThreadPool 批量创建前构造。
  explicit Thread(ThreadFunc func, const std::string& name = std::string());
  /// 含义：未 join 则 detach。角色：防止线程仍运行时已销毁 Thread 对象。
  ~Thread();

  /// 含义：pthread_create 启动线程。角色：ThreadPool::start、Logger 后台启动。
  void start();
  /// 含义：pthread_join 等待结束。角色：ThreadPool::stop 优雅退出。
  void join();

  /// 含义：是否已 start。角色：避免重复启动。
  bool started() const { return started_; }
  /// 含义：线程调试名。角色：日志与 top 中识别 Worker0、Logger 等。
  const std::string& name() const { return name_; }
  /// 含义：Linux 内核 TID。角色：调试、与 EventLoop::threadId_ 对比。
  pid_t tid() const { return tid_; }

 private:
  static void* startThread(void* obj);

  bool started_;         ///< 含义：启动标志。角色：start/join 状态机。
  bool joined_;          ///< 含义：已 join 标志。角色：析构时决定是否 detach。
  pthread_t pthreadId_;  ///< 含义：pthread 句柄。角色：join/detach 目标。
  pid_t tid_;            ///< 含义：内核线程 ID。角色：start 后由子线程写入。
  ThreadFunc func_;      ///< 含义：线程体。角色：workerLoop、backendLoop 等。
  std::string name_;     ///< 含义：显示用名称。角色：pthread_setname_np。
};

}  // namespace reactor
