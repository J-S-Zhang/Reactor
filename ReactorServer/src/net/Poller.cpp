#include "net/Poller.h"

#include "net/Channel.h"
#include "net/EpollPoller.h"
#include "net/EventLoop.h"

namespace reactor {

Poller::Poller(EventLoop* loop) : ownerLoop_(loop) {}

Poller::~Poller() = default;

/// 判断 fd 是否已在 poller 中注册
bool Poller::hasChannel(Channel* channel) const {
  return channels_.find(channel->fd()) != channels_.end();
}

/// Linux 下默认使用 EpollPoller
Poller* Poller::newDefaultPoller(EventLoop* loop) {
  return new EpollPoller(loop);
}

}  // namespace reactor
