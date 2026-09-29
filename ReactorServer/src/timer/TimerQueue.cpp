#include "timer/TimerQueue.h"

#include <sys/timerfd.h>
#include <unistd.h>

#include <utility>

#include "base/Logger.h"
#include "net/Channel.h"
#include "net/EventLoop.h"

namespace reactor {

namespace {
/// 做什么：timerfd_create 非阻塞定时器 fd。项目角色：并入 epoll 统一等待。
int createTimerfd() {
  int timerfd = ::timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
  if (timerfd < 0) {
    LOG_FATAL("timerfd_create failed");
  }
  return timerfd;
}

/// 做什么：计算距离 when 的 timespec。项目角色：timerfd_settime 参数。
struct timespec howMuchTimeFromNow(Timestamp when) {
  int64_t microseconds =
      when.microSecondsSinceEpoch() - Timestamp::now().microSecondsSinceEpoch();
  if (microseconds < 100) {
    microseconds = 100;
  }
  struct timespec ts;
  ts.tv_sec = static_cast<time_t>(microseconds / Timestamp::kMicroSecondsPerSecond);
  ts.tv_nsec = static_cast<long>(
      (microseconds % Timestamp::kMicroSecondsPerSecond) * 1000);
  return ts;
}

/// 做什么：设置 timerfd 下次到期。项目角色：插入更早定时器时更新内核定时。
void resetTimerfd(int timerfd, Timestamp expiration) {
  struct itimerspec newValue {};
  struct itimerspec oldValue {};
  newValue.it_value = howMuchTimeFromNow(expiration);
  if (::timerfd_settime(timerfd, 0, &newValue, &oldValue) < 0) {
    LOG_ERROR("timerfd_settime failed");
  }
}

/// 做什么：收集并 erase 所有到期 Timer*。项目角色：handleRead 执行回调前。
std::vector<Timer*> getExpired(TimerQueue::TimerList* timers, Timestamp now) {
  std::vector<Timer*> expired;
  auto it = timers->begin();
  while (it != timers->end() && it->first <= now) {
    expired.push_back(it->second);
    ++it;
  }
  for (Timer* timer : expired) {
    timers->erase(TimerQueue::Entry(timer->expiration(), timer));
  }
  return expired;
}

/// 做什么：重复定时器 restart 后重新 insert。项目角色：周期 idle 超时等。
void resetExpired(const std::vector<Timer*>& expired, Timestamp now,
                  TimerQueue::TimerList* timers) {
  for (Timer* timer : expired) {
    if (timer->repeat()) {
      timer->restart(now);
      timers->insert(TimerQueue::Entry(timer->expiration(), timer));
    }
  }
}
}  // namespace

/// 做什么：创建 timerfd Channel 并 enableReading。项目角色：EventLoop 构造子系统。
TimerQueue::TimerQueue(EventLoop* loop)
    : loop_(loop),
      timerfd_(createTimerfd()),
      timerfdChannel_(std::make_unique<Channel>(loop, timerfd_)),
      callingExpiredTimers_(false) {
  timerfdChannel_->setReadCallback([this](Timestamp) { handleRead(); });
  timerfdChannel_->enableReading();
}

/// 做什么：移除 channel、close fd、delete 所有 Timer。项目角色：进程退出清理。
TimerQueue::~TimerQueue() {
  timerfdChannel_->disableAll();
  timerfdChannel_->remove();
  ::close(timerfd_);
  for (const Entry& timer : timers_) {
    delete timer.second;
  }
}

/// 做什么：new Timer 并 runInLoop addTimerInLoop。项目角色：TcpConnection 空闲超时注册。
TimerId TimerQueue::addTimer(TimerCallback cb, Timestamp when,
                             int64_t interval) {
  Timer* timer = new Timer(std::move(cb), when, interval);
  loop_->runInLoop([this, timer] { addTimerInLoop(timer); });
  return timer;
}

/// 做什么：runInLoop cancelInLoop。项目角色：连接关闭 cancel idleTimer_。
void TimerQueue::cancel(TimerId timerId) {
  loop_->runInLoop([this, timerId] { cancelInLoop(timerId); });
}

/// 做什么：insert 并在最早变化时 resetTimerfd。项目角色：IO 线程实际注册定时器。
void TimerQueue::addTimerInLoop(Timer* timer) {
  loop_->assertInLoopThread();
  bool earliestChanged = insert(timer);
  if (earliestChanged) {
    resetTimerfd(timerfd_, timer->expiration());
  }
}

/// 做什么：从 timers_/activeTimers_ 删除并 delete。项目角色：取消 idle 等定时任务。
void TimerQueue::cancelInLoop(Timer* timer) {
  loop_->assertInLoopThread();
  ActiveTimer timerPair(timer, timer->interval());
  auto it = activeTimers_.find(timerPair);
  if (it == activeTimers_.end()) {
    return;
  }
  if (timers_.erase(Entry(timer->expiration(), timer)) > 0) {
    delete timer;
  }
  activeTimers_.erase(it);
}

/// 做什么：读 timerfd、执行到期回调、调度下一次 timerfd。项目角色：与网络事件同线程的定时驱动。
void TimerQueue::handleRead() {
  loop_->assertInLoopThread();
  uint64_t howmany;
  ssize_t n = ::read(timerfd_, &howmany, sizeof howmany);
  (void)n;
  Timestamp now(Timestamp::now());
  std::vector<Timer*> expired = getExpired(&timers_, now);

  callingExpiredTimers_ = true;
  cancelingTimers_.clear();
  for (Timer* timer : expired) {
    timer->run();
  }
  callingExpiredTimers_ = false;

  resetExpired(expired, now, &timers_);
  for (Timer* timer : expired) {
    if (!timer->repeat()) {
      ActiveTimer timerPair(timer, timer->interval());
      activeTimers_.erase(timerPair);
      delete timer;
    }
  }

  if (!timers_.empty()) {
    Timestamp nextExpire = timers_.begin()->first;
    resetTimerfd(timerfd_, nextExpire);
  }
}

/// 做什么：插入有序 set 与 activeTimers_。项目角色：维护 O(log n) 定时结构。
bool TimerQueue::insert(Timer* timer) {
  loop_->assertInLoopThread();
  bool earliestChanged = false;
  Timestamp when = timer->expiration();
  auto it = timers_.begin();
  if (it == timers_.end() || when < it->first) {
    earliestChanged = true;
  }
  timers_.insert(Entry(when, timer));
  activeTimers_.insert(ActiveTimer(timer, timer->interval()));
  return earliestChanged;
}

}  // namespace reactor
