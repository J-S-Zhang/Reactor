#pragma once

#include <condition_variable>
#include <deque>
#include <mutex>

namespace reactor {

template <typename T>
class TaskQueue {
 public:
  void push(T item) {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      queue_.push_back(std::move(item));
    }
    cond_.notify_one();
  }

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
  mutable std::mutex mutex_;
  std::condition_variable cond_;
  std::deque<T> queue_;
  bool stopped_{false};
};

}  // namespace reactor
