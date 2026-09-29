#include "net/Poller.h"

#include "net/Channel.h"
#include "net/EpollPoller.h"
#include "net/EventLoop.h"

namespace reactor {

Poller::Poller(EventLoop* loop) : ownerLoop_(loop) {}

Poller::~Poller() = default;

bool Poller::hasChannel(Channel* channel) const {
  return channels_.find(channel->fd()) != channels_.end();
}

Poller* Poller::newDefaultPoller(EventLoop* loop) {
  return new EpollPoller(loop);
}

}  // namespace reactor
