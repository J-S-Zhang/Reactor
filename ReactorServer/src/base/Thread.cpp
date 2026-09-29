#define _GNU_SOURCE 1
#include "base/Thread.h"

#include <unistd.h>

#include <stdexcept>

#include "base/Logger.h"

namespace reactor {

namespace {
/// 传给 pthread 入口的参数包
struct ThreadData {
  ThreadFunc func;
  std::string name;
  pid_t* tid;
};

/// pthread 实际入口：设置 tid/线程名后执行用户函数
void* threadRoutine(void* arg) {
  auto* data = static_cast<ThreadData*>(arg);
  *data->tid = gettid();
  if (!data->name.empty()) {
    pthread_setname_np(pthread_self(), data->name.substr(0, 15).c_str());
  }
  data->func();
  delete data;
  return nullptr;
}
}  // namespace

/// 构造线程对象，尚未启动
Thread::Thread(ThreadFunc func, const std::string& name)
    : started_(false),
      joined_(false),
      pthreadId_(0),
      tid_(0),
      func_(std::move(func)),
      name_(name) {}

/// 若已 start 但未 join，则 detach 避免泄漏
Thread::~Thread() {
  if (started_ && !joined_) {
    pthread_detach(pthreadId_);
  }
}

/// 创建并启动 pthread
void Thread::start() {
  if (started_) return;
  started_ = true;
  auto* data = new ThreadData{func_, name_, &tid_};
  if (pthread_create(&pthreadId_, nullptr, threadRoutine, data) != 0) {
    started_ = false;
    delete data;
    throw std::runtime_error("pthread_create failed");
  }
}

/// 等待线程结束
void Thread::join() {
  if (!started_ || joined_) return;
  joined_ = true;
  pthread_join(pthreadId_, nullptr);
}

}  // namespace reactor
