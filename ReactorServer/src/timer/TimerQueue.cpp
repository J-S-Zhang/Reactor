#include "timer/TimerQueue.h"

#include <sys/timerfd.h>
#include <unistd.h>

#include <utility>

#include "base/Logger.h"
#include "net/Channel.h"
#include "net/EventLoop.h"

namespace reactor {

namespace {
/// 创建非阻塞 timerfd
int createTimerfd() {
  int timerfd = ::timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
  if (timerfd < 0) {
    LOG_FATAL("timerfd_create failed");
  }
  return timerfd;
}

/// 计算距离 when 还有多久（至少 100us）
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

/// 重置 timerfd 的下一次到期时间
void resetTimerfd(int timerfd, Timestamp expiration) {
  struct itimerspec newValue {};
  struct itimerspec oldValue {};
  newValue.it_value = howMuchTimeFromNow(expiration);
  if (::timerfd_settime(timerfd, 0, &newValue, &oldValue) < 0) {
    LOG_ERROR("timerfd_settime failed");
  }
}

/// 取出所有 <= now 的定时器并从 timers 中删除
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

/// 重复定时器重新插入队列
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

/// 创建 timerfd 并注册到 EventLoop
TimerQueue::TimerQueue(EventLoop* loop)
    : loop_(loop),
      timerfd_(createTimerfd()),
      timerfdChannel_(std::make_unique<Channel>(loop, timerfd_)),
      callingExpiredTimers_(false) {
  timerfdChannel_->setReadCallback([this](Timestamp) { handleRead(); });
  timerfdChannel_->enableReading();
}

/// 注销 channel、关闭 fd、释放所有 Timer
TimerQueue::~TimerQueue() {
  timerfdChannel_->disableAll();
  timerfdChannel_->remove();
  ::close(timerfd_);
  for (const Entry& timer : timers_) {
    delete timer.second;
  }
}

/// 线程安全：在 loop 线程中插入定时器
TimerId TimerQueue::addTimer(TimerCallback cb, Timestamp when,
                             int64_t interval) {
  Timer* timer = new Timer(std::move(cb), when, interval);
  loop_->runInLoop([this, timer] { addTimerInLoop(timer); });
  return timer;
}

/// 线程安全：在 loop 线程中取消定时器
void TimerQueue::cancel(TimerId timerId) {
  loop_->runInLoop([this, timerId] { cancelInLoop(timerId); });
}

/// 在 IO 线程将定时器加入结构，必要时更新 timerfd
void TimerQueue::addTimerInLoop(Timer* timer) {
  loop_->assertInLoopThread();
  bool earliestChanged = insert(timer);
  if (earliestChanged) {
    resetTimerfd(timerfd_, timer->expiration());
  }
}

/// 在 IO 线程移除并 delete 定时器
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

/// timerfd 可读：执行到期任务并调度下一次 timerfd
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

/// 插入定时器；若新定时器最早则返回 true
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
