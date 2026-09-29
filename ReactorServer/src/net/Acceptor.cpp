#include "net/Acceptor.h"

#include "base/Logger.h"
#include "net/EventLoop.h"

namespace reactor {

/// 创建非阻塞 listen socket 并 bind
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

/// 在 IO 线程开始 listen 并关注可读（有新连接）
void Acceptor::listen() {
  loop_->assertInLoopThread();
  listenning_ = true;
  acceptSocket_.listen();
  acceptChannel_.enableReading();
}

/// ET 模式：循环 accept 直到 EAGAIN
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
