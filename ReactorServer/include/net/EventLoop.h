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

/// Reactor 核心：单线程事件循环 + 跨线程任务队列
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

  std::atomic<bool> looping_;     ///< 是否正在 loop() 中
  std::atomic<bool> quit_;        ///< 是否请求退出循环
  const pid_t threadId_;          ///< 创建 EventLoop 的线程 tid

  Timestamp pollReturnTime_;      ///< 最近一次 poll 返回时刻
  std::unique_ptr<Poller> poller_;
  std::unique_ptr<TimerQueue> timerQueue_;

  int wakeupFd_;                  ///< eventfd，用于唤醒 epoll_wait
  std::unique_ptr<Channel> wakeupChannel_;

  ChannelList activeChannels_;    ///< 本轮 poll 就绪的 Channel
  bool callingPendingFunctors_;   ///< 是否正在执行 pendingFunctors_
  std::vector<Functor> pendingFunctors_;  ///< 其他线程投递的回调
  std::mutex mutex_;              ///< 保护 pendingFunctors_
};

}  // namespace reactor
