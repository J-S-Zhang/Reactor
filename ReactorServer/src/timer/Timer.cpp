#include "timer/Timer.h"

namespace reactor {

/// 做什么：重复定时器将 expiration_ 设为 now+interval_。
/// 项目角色：TimerQueue 一次触发后重新入队。
void Timer::restart(Timestamp now) {
  if (repeat_) {
    expiration_ = Timestamp(now.microSecondsSinceEpoch() + interval_);
  }
}

}  // namespace reactor
