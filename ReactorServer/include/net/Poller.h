#pragma once

#include <map>
#include <vector>

#include "base/Timestamp.h"

namespace reactor {

class Channel;
class EventLoop;

class Poller {
 public:
  using ChannelList = std::vector<Channel*>;

  Poller(EventLoop* loop);
  virtual ~Poller();

  virtual Timestamp poll(int timeoutMs, ChannelList* activeChannels) = 0;
  virtual void updateChannel(Channel* channel) = 0;
  virtual void removeChannel(Channel* channel) = 0;

  static Poller* newDefaultPoller(EventLoop* loop);

  bool hasChannel(Channel* channel) const;

 protected:
  using ChannelMap = std::map<int, Channel*>;
  EventLoop* ownerLoop_;
  ChannelMap channels_;
};

}  // namespace reactor
