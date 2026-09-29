#include "net/Channel.h"

#include <poll.h>

#include "base/Logger.h"
#include "net/EventLoop.h"

namespace reactor {

const int Channel::kNoneEvent = 0;
const int Channel::kReadEvent = POLLIN | POLLPRI;
const int Channel::kWriteEvent = POLLOUT;

/// 做什么：初始化 fd 与事件状态，index_=kNew。
/// 项目角色：每个 socket/timerfd/eventfd 绑定一个 Channel。
Channel::Channel(EventLoop* loop, int fd)
    : loop_(loop),
      fd_(fd),
      events_(0),
      revents_(0),
      index_(-1) {}

/// 做什么：若仍注册事件则 WARN。
/// 项目角色：提醒用户应先 disableAll/remove。
Channel::~Channel() {
  if (events_ != kNoneEvent) {
    LOG_WARN("Channel::~Channel() fd=%d still registered", fd_);
  }
}

/// 做什么：按 revents 顺序调用 close/error/read/write 回调。
/// 项目角色：EventLoop 事件分发的最后一环。
void Channel::handleEvent(Timestamp receiveTime) {
  if (revents_ & POLLNVAL) {
    LOG_WARN("Channel::handleEvent() POLLNVAL fd=%d", fd_);
  }
  if ((revents_ & POLLHUP) && !(revents_ & POLLIN)) {
    if (closeCallback_) closeCallback_();
  }
  if (revents_ & (POLLERR | POLLNVAL)) {
    if (errorCallback_) errorCallback_();
  }
  if (revents_ & (POLLIN | POLLPRI | POLLRDHUP)) {
    if (readCallback_) readCallback_(receiveTime);
  }
  if (revents_ & POLLOUT) {
    if (writeCallback_) writeCallback_();
  }
}

/// 做什么：events_|=读并 update。项目角色：开始 accept/read。
void Channel::enableReading() {
  events_ |= kReadEvent;
  update();
}

void Channel::disableReading() {
  events_ &= ~kReadEvent;
  update();
}

/// 做什么：events_|=写并 update。项目角色：outputBuffer 有数据待写。
void Channel::enableWriting() {
  events_ |= kWriteEvent;
  update();
}

void Channel::disableWriting() {
  events_ &= ~kWriteEvent;
  update();
}

/// 做什么：清空 events_ 并 update。项目角色：关闭连接前注销 interest。
void Channel::disableAll() {
  events_ = kNoneEvent;
  update();
}

/// 做什么：调用 loop_->updateChannel。项目角色：同步 epoll 关注集。
void Channel::update() { loop_->updateChannel(this); }

/// 做什么：调用 loop_->removeChannel。项目角色：连接销毁从 epoll 删除 fd。
void Channel::remove() { loop_->removeChannel(this); }

}  // namespace reactor
