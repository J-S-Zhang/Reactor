#pragma once

#include <functional>

#include "base/NonCopyable.h"
#include "net/Channel.h"
#include "net/InetAddress.h"
#include "net/Socket.h"

namespace reactor {

class EventLoop;

/**
 * @class Acceptor
 * 含义：监听 TCP 端口并在有新连接时 accept。
 * 项目角色：TcpServer 内部组件；将「listen fd 可读」转为 connfd + peer 地址，
 *           交给 TcpServer::newConnection，是服务器入口的第一跳。
 */
class Acceptor : NonCopyable {
 public:
  using NewConnectionCallback =
      std::function<void(int sockfd, const InetAddress&)>;

  /// 含义：创建 listen socket 并 bind。角色：TcpServer 成员初始化。
  Acceptor(EventLoop* loop, const InetAddress& listenAddr, bool reuseport);
  ~Acceptor();

  /// 含义：设置新连接回调。角色：TcpServer 绑定 newConnection。
  void setNewConnectionCallback(NewConnectionCallback cb) {
    newConnectionCallback_ = std::move(cb);
  }

  /// 含义：是否已 listen 并监听读事件。角色：启动日志。
  bool listenning() const { return listenning_; }
  /// 含义：listen + enableReading。角色：TcpServer::start。
  void listen();

 private:
  /// 含义：listen fd 可读时循环 accept。角色：Channel 读回调。
  void handleRead();

  EventLoop* loop_;                         ///< 含义：IO 线程 loop。角色：Channel 所属。
  Socket acceptSocket_;                     ///< 含义：监听 socket。角色：bind/listen/accept。
  Channel acceptChannel_;                   ///< 含义：listen fd 事件通道。角色：epoll 注册。
  bool listenning_;                         ///< 含义：已启动监听。角色：状态位。
  NewConnectionCallback newConnectionCallback_;  ///< 含义：新 connfd 处理器。角色：交给 TcpServer。
};

}  // namespace reactor
