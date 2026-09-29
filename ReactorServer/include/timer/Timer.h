#pragma once

#include <functional>

#include "base/Timestamp.h"

namespace reactor {

/// 单个定时任务：到期时间、可选重复间隔、回调
class Timer {
 public:
  using TimerCallback = std::function<void()>;

  Timer(TimerCallback cb, Timestamp when, int64_t interval)
      : callback_(std::move(cb)),
        expiration_(when),
        interval_(interval),
        repeat_(interval > 0) {}

  void run() const {
    if (callback_) callback_();
  }

  Timestamp expiration() const { return expiration_; }
  bool repeat() const { return repeat_; }
  int64_t interval() const { return interval_; }

  void restart(Timestamp now);

 private:
  TimerCallback callback_;   ///< 到期时执行的函数
  Timestamp expiration_;     ///< 下次到期时刻
  const int64_t interval_;   ///< 重复间隔（微秒），0 表示单次
  const bool repeat_;        ///< 是否周期性定时器
};

}  // namespace reactor
