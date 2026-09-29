#pragma once

#include <condition_variable>
#include <deque>
#include <mutex>

namespace reactor {

/**
 * @class TaskQueue
 * 含义：线程安全的 FIFO 队列（mutex + condition_variable）。
 * 项目角色：ThreadPool 的任务队列、AsyncLogger 的日志行队列；
 *           实现方案中的「生产者-消费者」模型基础容器。
 */
template <typename T>
class TaskQueue {
 public:
  /// 含义：入队并 notify 一个等待者。角色：IO 线程 run(task)、log 入队。
  void push(T item) {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      queue_.push_back(std::move(item));
    }
    cond_.notify_one();
  }

  /// 含义：出队；timeoutMs<0 阻塞直到有数据或 stop。角色：worker/Logger 消费。
  /// 返回 false 表示超时或已 stop 且队列为空。
  bool pop(T& item, int timeoutMs = -1) {
    std::unique_lock<std::mutex> lock(mutex_);
    if (timeoutMs < 0) {
      cond_.wait(lock, [this] { return !queue_.empty() || stopped_; });
    } else {
      cond_.wait_for(lock, std::chrono::milliseconds(timeoutMs),
                     [this] { return !queue_.empty() || stopped_; });
    }
    if (queue_.empty()) return false;
    item = std::move(queue_.front());
    queue_.pop_front();
    return true;
  }

  /// 含义：标记停止并唤醒所有等待线程。角色：ThreadPool::stop、Logger 析构。
  void stop() {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      stopped_ = true;
    }
    cond_.notify_all();
  }

  /// 含义：当前队列长度。角色：Logger 析构时尽量排空。
  size_t size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return queue_.size();
  }

 private:
  mutable std::mutex mutex_;       ///< 含义：队列互斥锁。角色：保护 queue_ / stopped_。
  std::condition_variable cond_;   ///< 含义：条件变量。角色：pop 阻塞与 push 唤醒。
  std::deque<T> queue_;            ///< 含义：任务存储。角色：FIFO 缓冲。
  bool stopped_{false};            ///< 含义：停止标志。角色：让消费者线程退出 wait。
};

}  // namespace reactor
