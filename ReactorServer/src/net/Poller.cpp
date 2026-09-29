#include "net/Poller.h"

#include "net/Channel.h"
#include "net/EpollPoller.h"
#include "net/EventLoop.h"

namespace reactor {

/// 做什么：保存 ownerLoop_ 指针。
/// 项目角色：Poller 与 EventLoop 关联。
Poller::Poller(EventLoop* loop) : ownerLoop_(loop) {}

Poller::~Poller() = default;

/// 做什么：查 channels_ 是否包含 channel->fd()。
/// 项目角色：调试与一致性检查。
bool Poller::hasChannel(Channel* channel) const {
  return channels_.find(channel->fd()) != channels_.end();
}

/// 做什么：new EpollPoller(loop)。
/// 项目角色：Linux 下 EventLoop 默认 IO 后端。
Poller* Poller::newDefaultPoller(EventLoop* loop) {
  return new EpollPoller(loop);
}

}  // namespace reactor
