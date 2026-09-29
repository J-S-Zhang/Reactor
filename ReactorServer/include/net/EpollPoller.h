#pragma once

#include <vector>

#include "net/Poller.h"

struct epoll_event;

namespace reactor {

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

  int epollfd_;
  std::vector<struct epoll_event> events_;
};

}  // namespace reactor
