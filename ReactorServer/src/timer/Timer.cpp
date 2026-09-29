#include "timer/Timer.h"

namespace reactor {

/// 重复定时器：从 now 起加上 interval 计算下一次到期时间
void Timer::restart(Timestamp now) {
  if (repeat_) {
    expiration_ = Timestamp(now.microSecondsSinceEpoch() + interval_);
  }
}

}  // namespace reactor
