#include "net/Channel.h"

#include <poll.h>

#include "base/Logger.h"
#include "net/EventLoop.h"

namespace reactor {

const int Channel::kNoneEvent = 0;
const int Channel::kReadEvent = POLLIN | POLLPRI;
const int Channel::kWriteEvent = POLLOUT;

Channel::Channel(EventLoop* loop, int fd)
    : loop_(loop),
      fd_(fd),
      events_(0),
      revents_(0),
      index_(-1) {}

Channel::~Channel() {
  if (events_ != kNoneEvent) {
    LOG_WARN("Channel::~Channel() fd=%d still registered", fd_);
  }
}

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

void Channel::enableReading() {
  events_ |= kReadEvent;
  update();
}

void Channel::disableReading() {
  events_ &= ~kReadEvent;
  update();
}

void Channel::enableWriting() {
  events_ |= kWriteEvent;
  update();
}

void Channel::disableWriting() {
  events_ &= ~kWriteEvent;
  update();
}

void Channel::disableAll() {
  events_ = kNoneEvent;
  update();
}

void Channel::update() { loop_->updateChannel(this); }

void Channel::remove() { loop_->removeChannel(this); }

}  // namespace reactor
