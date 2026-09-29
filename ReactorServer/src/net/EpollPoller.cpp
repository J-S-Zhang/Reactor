#include "net/EpollPoller.h"

#include <cstring>
#include <errno.h>
#include <sys/epoll.h>
#include <unistd.h>

#include "base/Logger.h"
#include "net/Channel.h"
#include "net/EventLoop.h"

namespace reactor {

namespace {
const int kNew = -1;
const int kAdded = 1;
const int kDeleted = 2;
}  // namespace

/// 做什么：epoll_create1 并预分配 events_。
/// 项目角色：EventLoop 构造 poller_ 时创建 epoll 实例。
EpollPoller::EpollPoller(EventLoop* loop)
    : Poller(loop),
      epollfd_(::epoll_create1(EPOLL_CLOEXEC)),
      events_(kInitEventListSize) {
  if (epollfd_ < 0) {
    LOG_FATAL("epoll_create1 failed");
  }
}

/// 做什么：close(epollfd_)。
/// 项目角色：EventLoop 析构链释放 epoll。
EpollPoller::~EpollPoller() { ::close(epollfd_); }

/// 做什么：epoll_wait 填充 activeChannels，必要时扩容 events_。
/// 项目角色：Reactor「等待事件」核心 syscall。
Timestamp EpollPoller::poll(int timeoutMs, ChannelList* activeChannels) {
  int numEvents = ::epoll_wait(epollfd_, &*events_.begin(),
                               static_cast<int>(events_.size()), timeoutMs);
  int savedErrno = errno;
  Timestamp now(Timestamp::now());
  if (numEvents > 0) {
    fillActiveChannels(numEvents, activeChannels);
    if (static_cast<size_t>(numEvents) == events_.size()) {
      events_.resize(events_.size() * 2);
    }
  } else if (numEvents == 0) {
    (void)now;
  } else {
    if (savedErrno != EINTR) {
      LOG_ERROR("epoll_wait error");
    }
  }
  return now;
}

/// 做什么：从 events_[i].data.ptr 取 Channel 并 setRevents。
/// 项目角色：连接 epoll 内核回调与用户 Channel 对象。
void EpollPoller::fillActiveChannels(int numEvents,
                                     ChannelList* activeChannels) const {
  for (int i = 0; i < numEvents; ++i) {
    Channel* channel = static_cast<Channel*>(events_[i].data.ptr);
    channel->setRevents(events_[i].events);
    activeChannels->push_back(channel);
  }
}

/// 做什么：根据 Channel::index_ 决定 ADD/MOD/DEL。
/// 项目角色：Channel::update 的最终实现。
void EpollPoller::updateChannel(Channel* channel) {
  const int index = channel->index();
  if (index == kNew || index == kDeleted) {
    int fd = channel->fd();
    if (index == kNew) {
      if (channels_.find(fd) != channels_.end()) {
        LOG_FATAL("fd=%d already in channels_", fd);
      }
      channels_[fd] = channel;
    } else {
      if (channels_.find(fd) == channels_.end() || channels_[fd] != channel) {
        LOG_FATAL("updateChannel: fd mismatch");
      }
    }
    channel->setIndex(kAdded);
    update(EPOLL_CTL_ADD, channel);
  } else {
    if (channel->isNoneEvent()) {
      update(EPOLL_CTL_DEL, channel);
      channel->setIndex(kDeleted);
    } else {
      update(EPOLL_CTL_MOD, channel);
    }
  }
}

/// 做什么：从 channels_ 移除并可能 EPOLL_CTL_DEL。
/// 项目角色：TcpConnection::connectDestroyed。
void EpollPoller::removeChannel(Channel* channel) {
  int fd = channel->fd();
  if (channels_.find(fd) == channels_.end() || channels_[fd] != channel) {
    LOG_FATAL("removeChannel: fd not found");
  }
  int index = channel->index();
  if (index != kAdded && index != kDeleted) {
    LOG_FATAL("removeChannel: invalid index");
  }
  size_t n = channels_.erase(fd);
  (void)n;
  if (index == kAdded) {
    update(EPOLL_CTL_DEL, channel);
  }
  channel->setIndex(kNew);
}

/// 做什么：epoll_ctl 并强制 EPOLLET。
/// 项目角色：方案 11.2 边缘触发 + 非阻塞 IO。
void EpollPoller::update(int operation, Channel* channel) {
  struct epoll_event event {};
  std::memset(&event, 0, sizeof event);
  event.events = channel->events();
  event.data.ptr = channel;
  event.events |= EPOLLET;

  int fd = channel->fd();
  if (::epoll_ctl(epollfd_, operation, fd, &event) < 0) {
    if (operation == EPOLL_CTL_DEL) {
      LOG_ERROR("epoll_ctl del error fd=%d", fd);
    } else {
      LOG_FATAL("epoll_ctl op=%d fd=%d", operation, fd);
    }
  }
}

}  // namespace reactor
