#pragma once

#include <functional>
#include <memory>
#include <pthread.h>
#include <sys/types.h>
#include <unistd.h>

#include "base/NonCopyable.h"

namespace reactor {

/// 对 POSIX 线程的封装：创建、命名、join
class Thread : NonCopyable {
 public:
  using ThreadFunc = std::function<void()>;

  explicit Thread(ThreadFunc func, const std::string& name = std::string());
  ~Thread();

  void start();
  void join();

  bool started() const { return started_; }
  const std::string& name() const { return name_; }
  pid_t tid() const { return tid_; }

 private:
  static void* startThread(void* obj);

  bool started_;           ///< 是否已调用 start
  bool joined_;            ///< 是否已 join，避免重复 join
  pthread_t pthreadId_;    ///< pthread 句柄
  pid_t tid_;              ///< Linux 内核线程 ID（gettid）
  ThreadFunc func_;        ///< 线程入口要执行的函数
  std::string name_;       ///< 线程名（用于调试与 pthread_setname_np）
};

}  // namespace reactor
