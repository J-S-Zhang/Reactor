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

/// 基于 timerfd 的定时器集合，按到期时间排序，挂到 EventLoop
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

  EventLoop* loop_;                         ///< 所属事件循环
  const int timerfd_;                       ///< Linux timerfd 描述符
  std::unique_ptr<Channel> timerfdChannel_; ///< 将 timerfd 可读事件纳入 epoll
  TimerList timers_;                        ///< 按到期时间排序的定时器
  ActiveTimerSet activeTimers_;             ///< 便于按 Timer* 查找与取消
  bool callingExpiredTimers_;               ///< 是否正在执行到期回调（重入保护）
  ActiveTimerSet cancelingTimers_;          ///< 回调期间待取消的定时器（预留）
};

using TimerId = Timer*;

}  // namespace reactor
