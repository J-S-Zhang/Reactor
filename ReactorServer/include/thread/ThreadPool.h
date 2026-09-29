#pragma once

#include <functional>
#include <memory>
#include <vector>

#include "base/NonCopyable.h"
#include "base/Thread.h"
#include "thread/TaskQueue.h"

namespace reactor {

class ThreadPool : NonCopyable {
 public:
  using Task = std::function<void()>;

  explicit ThreadPool(const std::string& name = "ThreadPool");
  ~ThreadPool();

  void setThreadNum(int numThreads);
  void start(int numThreads);
  void stop();

  void run(Task task);

  size_t taskCount() const { return queue_.size(); }

 private:
  void workerLoop();

  std::string name_;
  TaskQueue<Task> queue_;
  std::vector<std::unique_ptr<Thread>> threads_;
  bool running_;
};

}  // namespace reactor
