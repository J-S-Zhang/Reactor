#pragma once

#include <functional>

#include "base/NonCopyable.h"
#include "net/Channel.h"
#include "net/InetAddress.h"
#include "net/Socket.h"

namespace reactor {

class EventLoop;

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

  EventLoop* loop_;
  Socket acceptSocket_;
  Channel acceptChannel_;
  bool listenning_;
  NewConnectionCallback newConnectionCallback_;
};

}  // namespace reactor
