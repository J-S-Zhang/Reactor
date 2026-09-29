#pragma once

#include <functional>

#include "base/NonCopyable.h"
#include "base/Timestamp.h"

namespace reactor {

class EventLoop;

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

  EventLoop* loop_;
  const int fd_;
  int events_;
  int revents_;
  int index_;

  ReadEventCallback readCallback_;
  EventCallback writeCallback_;
  EventCallback closeCallback_;
  EventCallback errorCallback_;
};

}  // namespace reactor
