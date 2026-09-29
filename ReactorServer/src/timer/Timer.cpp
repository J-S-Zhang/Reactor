#include "timer/Timer.h"

namespace reactor {

void Timer::restart(Timestamp now) {
  if (repeat_) {
    expiration_ = Timestamp(now.microSecondsSinceEpoch() + interval_);
  }
}

}  // namespace reactor
