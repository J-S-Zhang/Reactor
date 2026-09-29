#pragma once

#include <map>
#include <vector>

#include "base/Timestamp.h"

namespace reactor {

class Channel;
class EventLoop;

/**
 * @class Poller
 * 含义：IO 多路复用抽象接口（poll 一次、增删 Channel）。
 * 项目角色：Reactor 与具体 OS 机制（epoll）之间的策略层；
 *           EventLoop 只依赖 Poller，便于测试或将来换 poll/kqueue。
 */
class Poller {
 public:
  using ChannelList = std::vector<Channel*>;

  /// 含义：记录所属 EventLoop。角色：子类构造传递 ownerLoop_。
  Poller(EventLoop* loop);
  virtual ~Poller();

  /// 含义：等待就绪事件，填充 activeChannels。角色：EventLoop::loop 第一步。
  virtual Timestamp poll(int timeoutMs, ChannelList* activeChannels) = 0;
  /// 含义：新增或修改 Channel 关注事件。角色：Channel::update。
  virtual void updateChannel(Channel* channel) = 0;
  /// 含义：从 poller 移除 Channel。角色：Channel::remove。
  virtual void removeChannel(Channel* channel) = 0;

  /// 含义：工厂：Linux 返回 EpollPoller。角色：EventLoop 构造。
  static Poller* newDefaultPoller(EventLoop* loop);

  /// 含义：fd 是否已注册。角色：调试与断言。
  bool hasChannel(Channel* channel) const;

 protected:
  using ChannelMap = std::map<int, Channel*>;
  EventLoop* ownerLoop_;   ///< 含义：所属 loop。角色：断言在正确线程（子类可用）。
  ChannelMap channels_;    ///< 含义：fd→Channel。角色：EpollPoller 管理注册表。
};

}  // namespace reactor
