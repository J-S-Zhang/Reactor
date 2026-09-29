#pragma once

#include <functional>

#include "base/NonCopyable.h"
#include "net/Channel.h"
#include "net/InetAddress.h"
#include "net/Socket.h"

namespace reactor {

class EventLoop;

/// 在 listen fd 上 accept 新 TCP 连接
class Acceptor : NonCopyable {
 public:
  using NewConnectionCallback =
      std::function<void(int sockfd, const InetAddress&)>;

  Acceptor(EventLoop* loop, const InetAddress& listenAddr, bool reuseport);
  ~Acceptor();

  void setNewConnectionCallback(NewConnectionCallback cb) {
    newConnectionCallback_ = std::move(cb);
  }

  bool listenning() const { return listenning_; }
  void listen();

 private:
  void handleRead();

  EventLoop* loop_;                         ///< 所属 EventLoop
  Socket acceptSocket_;                     ///< 监听 socket
  Channel acceptChannel_;                   ///< 监听 fd 的 Channel
  bool listenning_;                         ///< 是否已 listen 并注册读事件
  NewConnectionCallback newConnectionCallback_;  ///< 新连接回调（通常交给 TcpServer）
};

}  // namespace reactor
