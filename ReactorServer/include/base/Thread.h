#pragma once

#include <functional>
#include <memory>
#include <pthread.h>
#include <sys/types.h>
#include <unistd.h>

#include "base/NonCopyable.h"

namespace reactor {

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

  bool started_;
  bool joined_;
  pthread_t pthreadId_;
  pid_t tid_;
  ThreadFunc func_;
  std::string name_;
};

}  // namespace reactor
