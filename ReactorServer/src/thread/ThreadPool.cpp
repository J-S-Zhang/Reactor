#include "thread/ThreadPool.h"

namespace reactor {

ThreadPool::ThreadPool(const std::string& name)
    : name_(name), running_(false) {}

ThreadPool::~ThreadPool() { stop(); }

/// 预留接口，当前由 start(numThreads) 指定线程数
void ThreadPool::setThreadNum(int numThreads) {
  (void)numThreads;
}

/// 创建 numThreads 个工作线程并开始消费队列
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

/// 停止队列并 join 所有工作线程
void ThreadPool::stop() {
  if (!running_) return;
  running_ = false;
  queue_.stop();
  for (auto& t : threads_) {
    t->join();
  }
  threads_.clear();
}

/// 提交任务到队列（线程安全）
void ThreadPool::run(Task task) { queue_.push(std::move(task)); }

/// 工作线程主循环：带超时 pop，避免无法退出
void ThreadPool::workerLoop() {
  Task task;
  while (running_) {
    if (queue_.pop(task, 500)) {
      if (task) task();
    }
  }
}

}  // namespace reactor
