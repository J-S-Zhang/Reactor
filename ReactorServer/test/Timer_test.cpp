/// Timer 重复间隔与 restart 行为测试
#include <cassert>
#include <iostream>

#include "base/Timestamp.h"
#include "timer/Timer.h"

int main() {
  reactor::Timestamp now = reactor::Timestamp::now();
  reactor::Timer timer([] {}, now, 1000 * 1000);
  assert(timer.repeat());
  timer.restart(now);
  assert(timer.expiration().microSecondsSinceEpoch() >=
         now.microSecondsSinceEpoch());
  std::cout << "Timer_test passed\n";
  return 0;
}
