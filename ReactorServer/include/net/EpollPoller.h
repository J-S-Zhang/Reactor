#pragma once

#include <vector>

#include "net/Poller.h"

struct epoll_event;

namespace reactor {

/**
 * @class EpollPoller
 * 含义：Linux epoll 实现的 Poller（含 EPOLLET 边缘触发）。
 * 项目角色：方案核心 IO 多路复用；EventLoop 默认 poller_ 即本类，
 *           支撑高并发 fd 监听与就绪事件批量返回。
 */
class EpollPoller : public Poller {
 public:
  explicit EpollPoller(EventLoop* loop);
  ~EpollPoller() override;

  Timestamp poll(int timeoutMs, ChannelList* activeChannels) override;
  void updateChannel(Channel* channel) override;
  void removeChannel(Channel* channel) override;

 private:
  static const int kInitEventListSize = 16;

  /// 含义：epoll_wait 结果转 Channel* 列表。角色：poll 后填充 activeChannels。
  void fillActiveChannels(int numEvents, ChannelList* activeChannels) const;
  /// 含义：封装 epoll_ctl。角色：ADD/MOD/DEL 统一 ET 设置。
  void update(int operation, Channel* channel);

  int epollfd_;                            ///< 含义：epoll 实例 fd。角色：epoll_wait/ctl 句柄。
  std::vector<struct epoll_event> events_; ///< 含义：wait 结果数组。角色：按需扩容。
};

}  // namespace reactor
