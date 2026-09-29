#pragma once

#include <functional>

#include "base/NonCopyable.h"
#include "base/Timestamp.h"

namespace reactor {

class EventLoop;

/// 封装 fd 及其关注的事件与回调，Reactor 中的「事件处理器」
class Channel : NonCopyable {
 public:
  using EventCallback = std::function<void()>;
  using ReadEventCallback = std::function<void(Timestamp)>;

  Channel(EventLoop* loop, int fd);
  ~Channel();

  void handleEvent(Timestamp receiveTime);
  void setReadCallback(ReadEventCallback cb) { readCallback_ = std::move(cb); }
  void setWriteCallback(EventCallback cb) { writeCallback_ = std::move(cb); }
  void setCloseCallback(EventCallback cb) { closeCallback_ = std::move(cb); }
  void setErrorCallback(EventCallback cb) { errorCallback_ = std::move(cb); }

  int fd() const { return fd_; }
  int events() const { return events_; }
  void setRevents(int revents) { revents_ = revents; }

  bool isNoneEvent() const { return events_ == kNoneEvent; }
  void enableReading();
  void disableReading();
  void enableWriting();
  void disableWriting();
  void disableAll();

  bool isWriting() const { return events_ & kWriteEvent; }
  bool isReading() const { return events_ & kReadEvent; }

  int index() const { return index_; }
  void setIndex(int idx) { index_ = idx; }

  EventLoop* ownerLoop() { return loop_; }

  static const int kNoneEvent;
  static const int kReadEvent;
  static const int kWriteEvent;

 private:
  void update();
  void remove();

  EventLoop* loop_;              ///< 所属 EventLoop
  const int fd_;                 ///< 监听的文件描述符
  int events_;                   ///< 希望监听的事件（POLLIN/OUT 等）
  int revents_;                  ///< poller 返回的就绪事件
  int index_;                    ///< 在 EpollPoller 中的状态（new/added/deleted）

  ReadEventCallback readCallback_;
  EventCallback writeCallback_;
  EventCallback closeCallback_;
  EventCallback errorCallback_;
};

}  // namespace reactor
