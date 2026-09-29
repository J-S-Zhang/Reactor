#define _GNU_SOURCE 1
#include "net/EventLoop.h"

#include <sys/eventfd.h>
#include <unistd.h>

#include <algorithm>

#include "base/Logger.h"
#include "net/Channel.h"
#include "net/Poller.h"
#include "timer/TimerQueue.h"

namespace reactor {

namespace {
thread_local EventLoop* t_loopInThisThread = nullptr;

int createEventfd() {
  int evtfd = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
  if (evtfd < 0) {
    LOG_FATAL("eventfd failed");
  }
  return evtfd;
}

const int kPollTimeMs = 10000;
}  // namespace

EventLoop* EventLoop::getEventLoopOfCurrentThread() {
  return t_loopInThisThread;
}

/// 初始化 poller、timer、wakeup channel，绑定到当前线程
EventLoop::EventLoop()
    : looping_(false),
      quit_(false),
      threadId_(gettid()),
      poller_(Poller::newDefaultPoller(this)),
      timerQueue_(new TimerQueue(this)),
      wakeupFd_(createEventfd()),
      wakeupChannel_(new Channel(this, wakeupFd_)),
      callingPendingFunctors_(false) {
  if (t_loopInThisThread) {
    LOG_FATAL("Another EventLoop exists in this thread");
  } else {
    t_loopInThisThread = this;
  }
  wakeupChannel_->setReadCallback([this](Timestamp) { handleRead(); });
  wakeupChannel_->enableReading();
}

EventLoop::~EventLoop() {
  wakeupChannel_->disableAll();
  wakeupChannel_->remove();
  ::close(wakeupFd_);
  t_loopInThisThread = nullptr;
}

/// Reactor 主循环：poll → 处理事件 → 执行 pending 任务
void EventLoop::loop() {
  assertInLoopThread();
  looping_ = true;
  quit_ = false;
  while (!quit_) {
    activeChannels_.clear();
    pollReturnTime_ = poller_->poll(kPollTimeMs, &activeChannels_);
    for (Channel* channel : activeChannels_) {
      channel->handleEvent(pollReturnTime_);
    }
    doPendingFunctors();
  }
  looping_ = false;
}

/// 请求退出；若在其他线程调用则 wakeup
void EventLoop::quit() {
  quit_ = true;
  if (!isInLoopThread()) {
    wakeup();
  }
}

/// 在 IO 线程立即执行，否则 queueInLoop
void EventLoop::runInLoop(Functor cb) {
  if (isInLoopThread()) {
    cb();
  } else {
    queueInLoop(std::move(cb));
  }
}

/// 将回调放入 pending 队列并必要时 wakeup
void EventLoop::queueInLoop(Functor cb) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    pendingFunctors_.push_back(std::move(cb));
  }
  if (!isInLoopThread() || callingPendingFunctors_) {
    wakeup();
  }
}

void EventLoop::updateChannel(Channel* channel) {
  assertInLoopThread();
  poller_->updateChannel(channel);
}

void EventLoop::removeChannel(Channel* channel) {
  assertInLoopThread();
  poller_->removeChannel(channel);
}

bool EventLoop::isInLoopThread() const { return threadId_ == gettid(); }

void EventLoop::assertInLoopThread() {
  if (!isInLoopThread()) {
    LOG_FATAL("EventLoop::assertInLoopThread failed");
  }
}

/// 向 eventfd 写 8 字节，使 epoll_wait 返回
void EventLoop::wakeup() {
  uint64_t one = 1;
  ssize_t n = ::write(wakeupFd_, &one, sizeof one);
  if (n != sizeof one) {
    LOG_ERROR("EventLoop::wakeup() writes %zd bytes instead of 8", n);
  }
}

/// 读 eventfd，清空计数
void EventLoop::handleRead() {
  uint64_t one = 1;
  ssize_t n = ::read(wakeupFd_, &one, sizeof one);
  if (n != sizeof one) {
    LOG_ERROR("EventLoop::handleRead() reads %zd bytes instead of 8", n);
  }
}

/// 批量执行其他线程投递的回调
void EventLoop::doPendingFunctors() {
  std::vector<Functor> functors;
  callingPendingFunctors_ = true;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    functors.swap(pendingFunctors_);
  }
  for (const Functor& functor : functors) {
    functor();
  }
  callingPendingFunctors_ = false;
}

}  // namespace reactor
