#pragma once

#include <atomic>
#include <functional>
#include <mutex>
#include <vector>

#include <memory>

#include "base/NonCopyable.h"
#include "base/Timestamp.h"
#include "net/Poller.h"

namespace reactor {

class Channel;
class TimerQueue;

class EventLoop : NonCopyable {
 public:
  using Functor = std::function<void()>;

  EventLoop();
  ~EventLoop();

  void loop();
  void quit();

  Timestamp pollReturnTime() const { return pollReturnTime_; }

  void runInLoop(Functor cb);
  void queueInLoop(Functor cb);

  void updateChannel(Channel* channel);
  void removeChannel(Channel* channel);

  bool isInLoopThread() const;
  void assertInLoopThread();

  void wakeup();
  void handleRead();

  TimerQueue* timerQueue() { return timerQueue_.get(); }

  static EventLoop* getEventLoopOfCurrentThread();

 private:
  void doPendingFunctors();

  std::atomic<bool> looping_;
  std::atomic<bool> quit_;
  const pid_t threadId_;

  Timestamp pollReturnTime_;
  std::unique_ptr<Poller> poller_;
  std::unique_ptr<TimerQueue> timerQueue_;

  int wakeupFd_;
  std::unique_ptr<Channel> wakeupChannel_;

  ChannelList activeChannels_;
  bool callingPendingFunctors_;
  std::vector<Functor> pendingFunctors_;
  std::mutex mutex_;
};

}  // namespace reactor
