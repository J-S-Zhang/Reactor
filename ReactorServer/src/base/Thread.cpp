#define _GNU_SOURCE 1
#include "base/Thread.h"

#include <unistd.h>

#include <stdexcept>

#include "base/Logger.h"

namespace reactor {

namespace {
struct ThreadData {
  ThreadFunc func;
  std::string name;
  pid_t* tid;
};

/// 做什么：pthread 入口，设置 tid/线程名并执行 func。
/// 项目角色：Thread::start 与 OS 线程的桥接。
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

/// 做什么：保存线程函数与名称，标记未启动。
/// 项目角色：ThreadPool 创建 worker 前的构造阶段。
Thread::Thread(ThreadFunc func, const std::string& name)
    : started_(false),
      joined_(false),
      pthreadId_(0),
      tid_(0),
      func_(std::move(func)),
      name_(name) {}

/// 做什么：若已 start 未 join 则 detach，避免 std::terminate。
/// 项目角色：异常路径下的资源安全。
Thread::~Thread() {
  if (started_ && !joined_) {
    pthread_detach(pthreadId_);
  }
}

/// 做什么：pthread_create 启动 threadRoutine。
/// 项目角色：ThreadPool、Logger、EventLoopThread 启动线程。
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

/// 做什么：pthread_join 等待线程结束。
/// 项目角色：ThreadPool::stop 同步回收 worker。
void Thread::join() {
  if (!started_ || joined_) return;
  joined_ = true;
  pthread_join(pthreadId_, nullptr);
}

}  // namespace reactor
