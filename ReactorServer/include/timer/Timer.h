#pragma once

#include <functional>

#include "base/Timestamp.h"

namespace reactor {

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
  TimerCallback callback_;
  Timestamp expiration_;
  const int64_t interval_;
  const bool repeat_;
};

}  // namespace reactor
