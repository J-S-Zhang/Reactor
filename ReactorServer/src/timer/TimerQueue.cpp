#include "timer/TimerQueue.h"

#include <sys/timerfd.h>
#include <unistd.h>

#include <utility>

#include "base/Logger.h"
#include "net/Channel.h"
#include "net/EventLoop.h"

namespace reactor {

namespace {
int createTimerfd() {
  int timerfd = ::timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
  if (timerfd < 0) {
    LOG_FATAL("timerfd_create failed");
  }
  return timerfd;
}

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

void resetTimerfd(int timerfd, Timestamp expiration) {
  struct itimerspec newValue {};
  struct itimerspec oldValue {};
  newValue.it_value = howMuchTimeFromNow(expiration);
  if (::timerfd_settime(timerfd, 0, &newValue, &oldValue) < 0) {
    LOG_ERROR("timerfd_settime failed");
  }
}

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

TimerQueue::TimerQueue(EventLoop* loop)
    : loop_(loop),
      timerfd_(createTimerfd()),
      timerfdChannel_(std::make_unique<Channel>(loop, timerfd_)),
      callingExpiredTimers_(false) {
  timerfdChannel_->setReadCallback([this](Timestamp) { handleRead(); });
  timerfdChannel_->enableReading();
}

TimerQueue::~TimerQueue() {
  timerfdChannel_->disableAll();
  timerfdChannel_->remove();
  ::close(timerfd_);
  for (const Entry& timer : timers_) {
    delete timer.second;
  }
}

TimerId TimerQueue::addTimer(TimerCallback cb, Timestamp when,
                             int64_t interval) {
  Timer* timer = new Timer(std::move(cb), when, interval);
  loop_->runInLoop([this, timer] { addTimerInLoop(timer); });
  return timer;
}

void TimerQueue::cancel(TimerId timerId) {
  loop_->runInLoop([this, timerId] { cancelInLoop(timerId); });
}

void TimerQueue::addTimerInLoop(Timer* timer) {
  loop_->assertInLoopThread();
  bool earliestChanged = insert(timer);
  if (earliestChanged) {
    resetTimerfd(timerfd_, timer->expiration());
  }
}

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
