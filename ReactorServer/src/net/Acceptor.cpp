#include "net/Acceptor.h"

#include "base/Logger.h"
#include "net/EventLoop.h"

namespace reactor {

/// 做什么：createNonblockingTcp + bind + 设置 accept 读回调。
/// 项目角色：TcpServer 内部创建监听器。
Acceptor::Acceptor(EventLoop* loop, const InetAddress& listenAddr,
                   bool reuseport)
    : loop_(loop),
      acceptSocket_(Socket::createNonblockingTcp()),
      acceptChannel_(loop, acceptSocket_.fd()),
      listenning_(false) {
  acceptSocket_.setReuseAddr(true);
  acceptSocket_.setReusePort(reuseport);
  acceptSocket_.bindAddress(listenAddr);
  acceptChannel_.setReadCallback([this](Timestamp) { handleRead(); });
}

Acceptor::~Acceptor() {
  acceptChannel_.disableAll();
  acceptChannel_.remove();
}

/// 做什么：listen 并 enableReading。
/// 项目角色：TcpServer::start 在 IO 线程调用。
void Acceptor::listen() {
  loop_->assertInLoopThread();
  listenning_ = true;
  acceptSocket_.listen();
  acceptChannel_.enableReading();
}

/// 做什么：循环 accept 直到失败(EAGAIN)。
/// 项目角色：ET 模式下一次事件接受所有就绪连接。
void Acceptor::handleRead() {
  loop_->assertInLoopThread();
  while (true) {
    InetAddress peerAddr;
    int connfd = acceptSocket_.accept(&peerAddr);
    if (connfd >= 0) {
      if (newConnectionCallback_) {
        newConnectionCallback_(connfd, peerAddr);
      } else {
        ::close(connfd);
      }
    } else {
      break;
    }
  }
}

}  // namespace reactor
