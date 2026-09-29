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

/// 做什么：创建非阻塞 eventfd。
/// 项目角色：EventLoop 跨线程 wakeup 机制。
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

/// 做什么：创建 poller、TimerQueue、wakeupChannel，绑定 thread_local。
/// 项目角色：main 或 EventLoopThread 启动 Reactor。
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

/// 做什么：while(!quit_) poll → handleEvent → doPendingFunctors。
/// 项目角色：进程主循环，驱动全部 IO 与定时器。
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

/// 做什么：quit_=true，非 IO 线程则 wakeup。
/// 项目角色：优雅停止服务器。
void EventLoop::quit() {
  quit_ = true;
  if (!isInLoopThread()) {
    wakeup();
  }
}

/// 做什么：IO 线程直接执行，否则 queueInLoop。
/// 项目角色：保证 Channel/socket 操作线程安全。
void EventLoop::runInLoop(Functor cb) {
  if (isInLoopThread()) {
    cb();
  } else {
    queueInLoop(std::move(cb));
  }
}

/// 做什么：加锁追加 pending 并可能 wakeup。
/// 项目角色：worker 线程关闭连接、send 等。
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

/// 做什么：write eventfd 唤醒 epoll_wait。
/// 项目角色：跨线程 queueInLoop 的关键。
void EventLoop::wakeup() {
  uint64_t one = 1;
  ssize_t n = ::write(wakeupFd_, &one, sizeof one);
  if (n != sizeof one) {
    LOG_ERROR("EventLoop::wakeup() writes %zd bytes instead of 8", n);
  }
}

/// 做什么：read eventfd 消费计数。
/// 项目角色：wakeupChannel 可读回调。
void EventLoop::handleRead() {
  uint64_t one = 1;
  ssize_t n = ::read(wakeupFd_, &one, sizeof one);
  if (n != sizeof one) {
    LOG_ERROR("EventLoop::handleRead() reads %zd bytes instead of 8", n);
  }
}

/// 做什么：swap 出 pending 并在 IO 线程逐个执行。
/// 项目角色：每轮 Reactor 处理跨线程任务。
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
