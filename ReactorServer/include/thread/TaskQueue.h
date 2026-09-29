#pragma once

#include <condition_variable>
#include <deque>
#include <mutex>

namespace reactor {

/// 线程安全任务队列（生产者-消费者）
template <typename T>
class TaskQueue {
 public:
  /// 入队并唤醒一个等待消费者
  void push(T item) {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      queue_.push_back(std::move(item));
    }
    cond_.notify_one();
  }

  /// 出队；timeoutMs<0 无限等待，否则超时返回 false
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

  /// 停止队列，唤醒所有等待线程
  void stop() {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      stopped_ = true;
    }
    cond_.notify_all();
  }

  size_t size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return queue_.size();
  }

 private:
  mutable std::mutex mutex_;       ///< 保护 queue_ 与 stopped_
  std::condition_variable cond_;   ///< 队列非空或停止时通知
  std::deque<T> queue_;            ///< 任务存储
  bool stopped_{false};            ///< 为 true 时 pop 不再阻塞等待新任务
};

}  // namespace reactor
