#pragma once

#include <functional>

#include "base/NonCopyable.h"
#include "base/Timestamp.h"

namespace reactor {

class EventLoop;

/**
 * @class Channel
 * 含义：一个 fd 在 Reactor 中的「事件订阅 + 回调」绑定。
 * 项目角色：Reactor 模式里的 Handler 抽象；TcpConnection、Acceptor、timerfd、
 *           eventfd 均通过 Channel 注册到 EpollPoller，是事件分发的最小单元。
 */
class Channel : NonCopyable {
 public:
  using EventCallback = std::function<void()>;
  using ReadEventCallback = std::function<void(Timestamp)>;

  /// 含义：绑定 loop 与 fd，初始不关注任何事件。角色：每个 fd 一个 Channel。
  Channel(EventLoop* loop, int fd);
  ~Channel();

  /// 含义：根据 poller 设置的 revents_ 调用读/写/关闭/错误回调。角色：EventLoop::loop 内分发。
  void handleEvent(Timestamp receiveTime);
  void setReadCallback(ReadEventCallback cb) { readCallback_ = std::move(cb); }
  void setWriteCallback(EventCallback cb) { writeCallback_ = std::move(cb); }
  void setCloseCallback(EventCallback cb) { closeCallback_ = std::move(cb); }
  void setErrorCallback(EventCallback cb) { errorCallback_ = std::move(cb); }

  int fd() const { return fd_; }
  /// 含义：希望监听的事件位。角色：epoll_ctl 的 events。
  int events() const { return events_; }
  /// 含义：设置本次 poll 返回的就绪事件。角色：EpollPoller::fillActiveChannels。
  void setRevents(int revents) { revents_ = revents; }

  /// 含义：是否未订阅任何事件。角色：EpollPoller 决定 DEL 还是 MOD。
  bool isNoneEvent() const { return events_ == kNoneEvent; }
  /// 含义：订阅可读。角色：connectEstablished、Acceptor::listen。
  void enableReading();
  void disableReading();
  /// 含义：订阅可写。角色：outputBuffer_ 有数据待发送。
  void enableWriting();
  void disableWriting();
  /// 含义：取消所有订阅。角色：关闭连接前。
  void disableAll();

  bool isWriting() const { return events_ & kWriteEvent; }
  bool isReading() const { return events_ & kReadEvent; }

  /// 含义：在 EpollPoller 中的注册状态。角色：kNew/kAdded/kDeleted 状态机。
  int index() const { return index_; }
  void setIndex(int idx) { index_ = idx; }

  /// 含义：所属 EventLoop。角色：update/remove 回调目标。
  EventLoop* ownerLoop() { return loop_; }

  static const int kNoneEvent;  ///< 含义：无事件。角色：disableAll。
  static const int kReadEvent;  ///< 含义：POLLIN|POLLPRI。角色：读/accept。
  static const int kWriteEvent; ///< 含义：POLLOUT。角色：写。

 private:
  /// 含义：请求 poller 更新 interest。角色：enable/disable 后调用。
  void update();
  /// 含义：从 poller 移除。角色：connectDestroyed。
  void remove();

  EventLoop* loop_;              ///< 含义：所属 Reactor。角色：updateChannel 路由。
  const int fd_;                 ///< 含义：关联描述符。角色：epoll 监控对象。
  int events_;                   ///< 含义：关注的事件掩码。角色：epoll_ctl 输入。
  int revents_;                  ///< 含义：就绪事件掩码。角色：handleEvent 判断。
  int index_;                    ///< 含义：epoll 注册状态。角色：EpollPoller::updateChannel。

  ReadEventCallback readCallback_;    ///< 含义：可读回调。角色：handleRead/accept/timer。
  EventCallback writeCallback_;       ///< 含义：可写回调。角色：handleWrite。
  EventCallback closeCallback_;       ///< 含义：对端关闭等。角色：handleClose。
  EventCallback errorCallback_;       ///< 含义：错误事件。角色：handleError。
};

}  // namespace reactor
