#include "thread/ThreadPool.h"

namespace reactor {

ThreadPool::ThreadPool(const std::string& name)
    : name_(name), running_(false) {}

ThreadPool::~ThreadPool() { stop(); }

void ThreadPool::setThreadNum(int numThreads) {
  (void)numThreads;
}

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

void ThreadPool::stop() {
  if (!running_) return;
  running_ = false;
  queue_.stop();
  for (auto& t : threads_) {
    t->join();
  }
  threads_.clear();
}

void ThreadPool::run(Task task) { queue_.push(std::move(task)); }

void ThreadPool::workerLoop() {
  Task task;
  while (running_) {
    if (queue_.pop(task, 500)) {
      if (task) task();
    }
  }
}

}  // namespace reactor
