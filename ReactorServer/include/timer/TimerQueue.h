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

/**
 * @class TimerQueue
 * 含义：按到期时间管理的定时器集合，通过 timerfd 并入 epoll。
 * 项目角色：挂接在 EventLoop 上，与网络事件同一线程调度；
 *           支撑 TcpConnection 空闲超时，实现方案定时器模块。
 */
class TimerQueue : NonCopyable {
 public:
  using TimerCallback = Timer::TimerCallback;

  /// 含义：创建 timerfd 并注册到 loop。角色：EventLoop 构造子对象。
  explicit TimerQueue(EventLoop* loop);
  ~TimerQueue();

  /// 含义：添加定时器（线程安全，内部 runInLoop）。角色：setIdleTimeout 等。
  /// 返回 TimerId 用于 cancel。
  TimerId addTimer(TimerCallback cb, Timestamp when, int64_t interval);
  /// 含义：取消定时器。角色：连接关闭时 cancel idleTimer_。
  void cancel(TimerId timerId);

  /// 含义：timerfd 可读时调用。角色：Channel 读回调，执行到期任务。
  void handleRead();

 private:
  using Entry = std::pair<Timestamp, Timer*>;
  using TimerList = std::set<Entry>;
  using ActiveTimer = std::pair<Timer*, int64_t>;
  using ActiveTimerSet = std::set<ActiveTimer>;

  /// 含义：在 IO 线程插入 timer。角色：addTimer 的实际逻辑。
  void addTimerInLoop(Timer* timer);
  /// 含义：在 IO 线程移除 timer。角色：cancel 的实际逻辑。
  void cancelInLoop(Timer* timer);

  /// 含义：加入 timers_/activeTimers_。角色：返回是否成为最早定时器。
  bool insert(Timer* timer);

  EventLoop* loop_;                         ///< 含义：所属 Reactor。角色：assertInLoop、runInLoop。
  const int timerfd_;                       ///< 含义：Linux 定时器 fd。角色：epoll 统一等待。
  std::unique_ptr<Channel> timerfdChannel_; ///< 含义：timerfd 的 Channel。角色：可读事件分发。
  TimerList timers_;                        ///< 含义：按 expiration 排序。角色：取最早到期。
  ActiveTimerSet activeTimers_;             ///< 含义：(Timer*,interval) 索引。角色：O(log n) cancel。
  bool callingExpiredTimers_;               ///< 含义：正在执行回调。角色：重入/cancel 语义预留。
  ActiveTimerSet cancelingTimers_;          ///< 含义：回调中待取消集合。角色：扩展安全 cancel。
};

/// 含义：定时器句柄类型。角色：TcpConnection::idleTimer_ 存储。
using TimerId = Timer*;

}  // namespace reactor
