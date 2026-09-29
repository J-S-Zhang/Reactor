#pragma once

#include <functional>
#include <memory>
#include <vector>

#include "base/NonCopyable.h"
#include "base/Thread.h"
#include "thread/TaskQueue.h"

namespace reactor {

/// 固定数量工作线程 + 任务队列，用于业务与 IO 解耦
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

  std::string name_;                              ///< 线程名前缀
  TaskQueue<Task> queue_;                         ///< 待执行任务
  std::vector<std::unique_ptr<Thread>> threads_;  ///< 工作线程列表
  bool running_;                                  ///< 是否已 start 且未 stop
};

}  // namespace reactor
