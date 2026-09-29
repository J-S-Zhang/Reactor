#include "thread/ThreadPool.h"

namespace reactor {

/// 做什么：初始化空池，running_=false。
/// 项目角色：TcpServer 构造 workerPool_。
ThreadPool::ThreadPool(const std::string& name)
    : name_(name), running_(false) {}

/// 做什么：stop 回收线程。
/// 项目角色：TcpServer 析构或进程退出。
ThreadPool::~ThreadPool() { stop(); }

/// 做什么：占位，线程数由 start 指定。
/// 项目角色：预留与配置文件对齐。
void ThreadPool::setThreadNum(int numThreads) {
  (void)numThreads;
}

/// 做什么：创建 numThreads 个 Thread 执行 workerLoop。
/// 项目角色：TcpServer::start 启动业务并发。
void ThreadPool::start(int numThreads) {
  if (running_) return;
  running_ = true;
  for (int i = 0; i < numThreads; ++i) {
    auto t = std::make_unique<Thread>(
        [this] { workerLoop(); }, name_ + std::to_string(i));
    t->start();
    threads_.push_back(std::move(t));
  }
}

/// 做什么：queue_.stop + join 所有 worker。
/// 项目角色：优雅关闭业务线程。
void ThreadPool::stop() {
  if (!running_) return;
  running_ = false;
  queue_.stop();
  for (auto& t : threads_) {
    t->join();
  }
  threads_.clear();
}

/// 做什么：任务入队。
/// 项目角色：TcpConnection 将 messageCallback 投递到业务线程。
void ThreadPool::run(Task task) { queue_.push(std::move(task)); }

/// 做什么：带超时 pop 并执行任务。
/// 项目角色：worker 主循环；超时以便检测 running_=false。
void ThreadPool::workerLoop() {
  Task task;
  while (running_) {
    if (queue_.pop(task, 500)) {
      if (task) task();
    }
  }
}

}  // namespace reactor
