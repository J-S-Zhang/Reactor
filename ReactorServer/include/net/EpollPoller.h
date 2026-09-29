#pragma once

#include <vector>

#include "net/Poller.h"

struct epoll_event;

namespace reactor {

/// epoll 实现的 Poller（边缘触发 ET）
class EpollPoller : public Poller {
 public:
  explicit EpollPoller(EventLoop* loop);
  ~EpollPoller() override;

  Timestamp poll(int timeoutMs, ChannelList* activeChannels) override;
  void updateChannel(Channel* channel) override;
  void removeChannel(Channel* channel) override;

 private:
  static const int kInitEventListSize = 16;

  void fillActiveChannels(int numEvents, ChannelList* activeChannels) const;
  void update(int operation, Channel* channel);

  int epollfd_;                           ///< epoll 实例 fd
  std::vector<struct epoll_event> events_; ///< epoll_wait 结果缓冲
};

}  // namespace reactor
