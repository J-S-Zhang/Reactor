#pragma once

#include <memory>
#include <set>
#include <vector>

#include "base/NonCopyable.h"
#include "base/Timestamp.h"
#include "timer/Timer.h"

namespace reactor {

class EventLoop;
class Channel;

class TimerQueue : NonCopyable {
 public:
  using TimerCallback = Timer::TimerCallback;

  explicit TimerQueue(EventLoop* loop);
  ~TimerQueue();

  TimerId addTimer(TimerCallback cb, Timestamp when, int64_t interval);
  void cancel(TimerId timerId);

  void handleRead();

 private:
  using Entry = std::pair<Timestamp, Timer*>;
  using TimerList = std::set<Entry>;
  using ActiveTimer = std::pair<Timer*, int64_t>;
  using ActiveTimerSet = std::set<ActiveTimer>;

  void addTimerInLoop(Timer* timer);
  void cancelInLoop(Timer* timer);

  bool insert(Timer* timer);

  EventLoop* loop_;
  const int timerfd_;
  std::unique_ptr<Channel> timerfdChannel_;
  TimerList timers_;
  ActiveTimerSet activeTimers_;
  bool callingExpiredTimers_;
  ActiveTimerSet cancelingTimers_;
};

using TimerId = Timer*;

}  // namespace reactor
