#pragma once

#include "base/NonCopyable.h"

namespace reactor {

class InetAddress;

/**
 * @class Socket
 * 含义：一个 TCP socket fd 的 RAII 与常用 syscall 封装。
 * 项目角色：Acceptor 的 listen fd、TcpConnection 的连接 fd；
 *           统一非阻塞、reuse、keepalive，落实方案「非阻塞 Socket」设计。
 */
class Socket : NonCopyable {
 public:
  /// 含义：接管已有 fd（不 create）。角色：accept 返回的 connfd 包装。
  explicit Socket(int sockfd) : sockfd_(sockfd) {}
  /// 含义：close fd。角色：连接销毁释放内核资源。
  ~Socket();

  /// 含义：返回 fd。角色：Channel 构造、read/write。
  int fd() const { return sockfd_; }

  /// 含义：bind。角色：Acceptor 绑定监听端口。
  void bindAddress(const InetAddress& addr);
  /// 含义：listen。角色：Acceptor::listen。
  void listen();
  /// 含义：accept4 非阻塞接受。角色：Acceptor::handleRead。
  int accept(InetAddress* peeraddr);
  /// 含义：关闭写半连接。角色：优雅关闭 TcpConnection::shutdownInLoop。

  void shutdownWrite();

  /// 含义：TCP_NODELAY。角色：降低小包延迟（可按需开启）。
  void setTcpNoDelay(bool on);
  /// 含义：SO_REUSEADDR。角色：重启服务快速 bind。
  void setReuseAddr(bool on);
  /// 含义：SO_REUSEPORT。角色：多进程监听同一端口（可选）。
  void setReusePort(bool on);
  /// 含义：SO_KEEPALIVE。角色：TcpConnection 构造时开启。
  void setKeepAlive(bool on);
  /// 含义：fcntl O_NONBLOCK。角色：与 epoll ET 配合。
  void setNonBlocking();

  /// 含义：创建 TCP 非阻塞 socket。角色：Acceptor 监听 socket 创建。
  static int createNonblockingTcp();

 private:
  const int sockfd_;  ///< 含义：内核 socket 描述符。角色：全连接 IO 的标识；析构 close。
};

}  // namespace reactor
