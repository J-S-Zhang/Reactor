#include "net/TcpServer.h"

#include <functional>
#include <sys/socket.h>

#include "base/Logger.h"
#include "net/EventLoop.h"

namespace reactor {

/// 做什么：构造 Acceptor 并绑定 newConnection 回调。
/// 项目角色：应用层创建 server 对象（echo/chat main）。
TcpServer::TcpServer(EventLoop* loop, const InetAddress& listenAddr,
                     const std::string& nameArg)
    : loop_(loop),
      name_(nameArg),
      acceptor_(loop, listenAddr, false),
      threadPool_(nullptr),
      workerPool_(std::make_unique<ThreadPool>("Worker")),
      started_(0),
      nextConnId_(1),
      idleTimeoutSeconds_(0),
      workerThreadNum_(4) {
  acceptor_.setNewConnectionCallback(
      [this](int fd, const InetAddress& peer) { newConnection(fd, peer); });
}

/// 做什么：遍历 connections_ 并在 loop 中 connectDestroyed。
/// 项目角色：须在 IO 线程析构 server 时清理 fd。
TcpServer::~TcpServer() {
  loop_->assertInLoopThread();
  for (auto& item : connections_) {
    TcpConnectionPtr conn(item.second);
    item.second.reset();
    conn->getLoop()->runInLoop([conn] { conn->connectDestroyed(); });
  }
}

void TcpServer::setThreadNum(int numThreads) {
  (void)numThreads;
}

/// 做什么：首次 start 时启动 workerPool_ 并在 IO 线程 listen。
/// 项目角色：main 中 server.start() 后 loop.loop()。
void TcpServer::start() {
  if (started_.exchange(1) == 0) {
    if (workerThreadNum_ > 0) {
      workerPool_->start(workerThreadNum_);
    }
    loop_->runInLoop([this] {
      acceptor_.listen();
      LOG_INFO("%s listening on %s", name_.c_str(),
               acceptor_.listenning() ? "yes" : "no");
    });
  }
}

/// 做什么：创建 TcpConnection、入 map、设回调、connectEstablished。
/// 项目角色：Acceptor 与 TcpConnection 之间的桥梁。
void TcpServer::newConnection(int sockfd, const InetAddress& peerAddr) {
  loop_->assertInLoopThread();
  char buf[64];
  std::snprintf(buf, sizeof buf, "%s#%d", name_.c_str(), nextConnId_);
  ++nextConnId_;
  InetAddress localAddr;
  struct sockaddr_in local {};
  socklen_t addrlen = sizeof local;
  if (::getsockname(sockfd, reinterpret_cast<struct sockaddr*>(&local),
                    &addrlen) == 0) {
    localAddr.setSockAddrInet(local);
  }
  TcpConnectionPtr conn(new TcpConnection(loop_, buf, sockfd, localAddr,
                                          peerAddr));
  connections_[buf] = conn;
  if (workerThreadNum_ > 0) {
    conn->setThreadPool(workerPool_.get());
  }
  conn->setConnectionCallback(connectionCallback_);
  conn->setMessageCallback(messageCallback_);
  conn->setWriteCompleteCallback(writeCompleteCallback_);
  conn->setCloseCallback(
      [this](const TcpConnectionPtr& c) { removeConnection(c); });
  if (idleTimeoutSeconds_ > 0) {
    conn->setIdleTimeout(idleTimeoutSeconds_);
  }
  conn->connectEstablished();
}

/// 做什么：runInLoop 到 removeConnectionInLoop。
/// 项目角色：TcpConnection closeCallback 通知 server 删连接。
void TcpServer::removeConnection(const TcpConnectionPtr& conn) {
  loop_->runInLoop([this, conn] { removeConnectionInLoop(conn); });
}

/// 做什么：erase map 并 connectDestroyed。
/// 项目角色：连接生命周期在 server 层的收尾。
void TcpServer::removeConnectionInLoop(const TcpConnectionPtr& conn) {
  loop_->assertInLoopThread();
  connections_.erase(conn->name());
  conn->connectDestroyed();
}

}  // namespace reactor
