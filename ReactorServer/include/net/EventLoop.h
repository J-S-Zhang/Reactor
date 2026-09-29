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

/**
 * @class EventLoop
 * 含义：单线程 Reactor 事件循环（poll → 分发 → 执行跨线程任务）。
 * 项目角色：整个框架的心脏；main 中 loop.loop()；TcpServer/Acceptor/Connection
 *           均运行在其线程；通过 runInLoop 保证 Channel 只在 IO 线程修改。
 */
class EventLoop : NonCopyable {
 public:
  using Functor = std::function<void()>;

  EventLoop();
  ~EventLoop();

  /// 含义：阻塞运行直到 quit。角色：进程主线程或 EventLoopThread 入口。
  void loop();
  /// 含义：请求退出 loop。角色：信号处理或析构子线程。
  void quit();

  /// 含义：最近一次 epoll_wait 返回时间。角色：传给 Channel 读回调。
  Timestamp pollReturnTime() const { return pollReturnTime_; }

  /// 含义：在 IO 线程立即执行，否则 queueInLoop。角色：TcpConnection::send 等。
  void runInLoop(Functor cb);
  /// 含义：将回调放入 pending 队列并 wakeup。角色：worker 线程操作连接。
  void queueInLoop(Functor cb);

  /// 含义：转发给 poller_。角色：Channel::update。
  void updateChannel(Channel* channel);
  /// 含义：转发给 poller_。角色：Channel::remove。
  void removeChannel(Channel* channel);

  /// 含义：当前线程是否为创建 loop 的线程。角色：runInLoop 判断。
  bool isInLoopThread() const;
  /// 含义：否则 LOG_FATAL。角色：保护 IO 对象线程亲和性。
  void assertInLoopThread();

  /// 含义：写 eventfd 唤醒 epoll_wait。角色：queueInLoop 跨线程。
  void wakeup();
  /// 含义：读 eventfd 清空计数。角色：wakeupChannel_ 读回调。

  void handleRead();

  /// 含义：访问定时器子系统。角色：TcpConnection 空闲超时。
  TimerQueue* timerQueue() { return timerQueue_.get(); }

  /// 含义：thread_local 当前线程 loop。角色：扩展多 loop 场景。
  static EventLoop* getEventLoopOfCurrentThread();

 private:
  /// 含义：交换并执行 pendingFunctors_。角色：每轮 poll 结束后。
  void doPendingFunctors();

  std::atomic<bool> looping_;     ///< 含义：是否在 loop() 中。角色：状态查询。
  std::atomic<bool> quit_;        ///< 含义：退出标志。角色：while 循环条件。
  const pid_t threadId_;          ///< 含义：IO 线程 tid。角色：isInLoopThread。

  Timestamp pollReturnTime_;      ///< 含义：poll 返回时刻。角色：事件时间戳。
  std::unique_ptr<Poller> poller_;           ///< 含义：epoll 封装。角色：等待 IO。
  std::unique_ptr<TimerQueue> timerQueue_;   ///< 含义：定时器队列。角色：超时/心跳。

  int wakeupFd_;                             ///< 含义：eventfd。角色：跨线程唤醒。
  std::unique_ptr<Channel> wakeupChannel_;   ///< 含义：eventfd 的 Channel。角色：纳入 epoll。

  ChannelList activeChannels_;    ///< 含义：本轮就绪 Channel 列表。角色：每轮 clear 后填充。
  bool callingPendingFunctors_;   ///< 含义：正在跑 pending。角色：避免 wakeup 死循环。
  std::vector<Functor> pendingFunctors_;  ///< 含义：其他线程投递的回调。角色：IO 线程执行。
  std::mutex mutex_;              ///< 含义：保护 pendingFunctors_。角色：queueInLoop 加锁。
};

}  // namespace reactor
