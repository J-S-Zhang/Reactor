#include "net/TcpConnection.h"

#include <errno.h>
#include <unistd.h>

#include "base/Logger.h"
#include "net/Channel.h"
#include "net/EventLoop.h"
#include "net/Socket.h"
#include "thread/ThreadPool.h"

namespace reactor {

/// 做什么：创建 Socket/Channel 并绑定四类 IO 回调。
/// 项目角色：TcpServer::newConnection 包装 accept 得到的 connfd。
TcpConnection::TcpConnection(EventLoop* loop, const std::string& name,
                             int sockfd, const InetAddress& localAddr,
                             const InetAddress& peerAddr)
    : loop_(loop),
      name_(name),
      state_(kConnecting),
      reading_(true),
      socket_(new Socket(sockfd)),
      channel_(new Channel(loop, sockfd)),
      localAddr_(localAddr),
      peerAddr_(peerAddr),
      threadPool_(nullptr),
      idleTimer_(nullptr) {
  channel_->setReadCallback([this](Timestamp t) { handleRead(t); });
  channel_->setWriteCallback([this] { handleWrite(); });
  channel_->setCloseCallback([this] { handleClose(); });
  channel_->setErrorCallback([this] { handleError(); });
  socket_->setKeepAlive(true);
}

/// 做什么：记录连接销毁日志。
/// 项目角色：shared_ptr 最后一个引用释放时 socket 由 Socket 析构 close。
TcpConnection::~TcpConnection() {
  LOG_DEBUG("TcpConnection::~TcpConnection() %s", name_.c_str());
}

/// 做什么：若已连接则 runInLoop/sendInLoop 发送字符串。
/// 项目角色：Codec、chat 广播从业务线程安全回写客户端。
void TcpConnection::send(const std::string& message) {
  if (state_ == kConnected) {
    if (loop_->isInLoopThread()) {
      sendInLoop(message);
    } else {
      loop_->runInLoop([this, message] { sendInLoop(message); });
    }
  }
}

/// 做什么：将 Buffer 可读区 send 并 retrieveAll。
/// 项目角色：上层已有组包 Buffer 时使用。
void TcpConnection::send(Buffer* buf) {
  if (state_ == kConnected) {
    if (loop_->isInLoopThread()) {
      sendInLoop(buf->peek(), buf->readableBytes());
      buf->retrieveAll();
    } else {
      loop_->runInLoop([this, buf] {
        sendInLoop(buf->peek(), buf->readableBytes());
        buf->retrieveAll();
      });
    }
  }
}

/// 做什么：转调 sendInLoop(data,len)。
/// 项目角色：send(string) 在 IO 线程内的实现入口。
void TcpConnection::sendInLoop(const std::string& message) {
  sendInLoop(message.data(), message.size());
}

/// 做什么：直接 write 或缓存到 outputBuffer_ 并关注 POLLOUT。
/// 项目角色：非阻塞写路径，Reactor 写事件驱动续写。
void TcpConnection::sendInLoop(const void* data, size_t len) {
  loop_->assertInLoopThread();
  ssize_t nwrote = 0;
  size_t remaining = len;
  bool faultError = false;

  if (state_ == kDisconnected) {
    LOG_WARN("disconnected, give up writing");
    return;
  }

  if (!channel_->isWriting() && outputBuffer_.readableBytes() == 0) {
    nwrote = ::write(channel_->fd(), data, len);
    if (nwrote >= 0) {
      remaining = len - static_cast<size_t>(nwrote);
      if (remaining == 0 && writeCompleteCallback_) {
        loop_->queueInLoop([this] {
          writeCompleteCallback_(shared_from_this());
        });
      }
    } else {
      nwrote = 0;
      if (errno != EWOULDBLOCK) {
        LOG_ERROR("TcpConnection::sendInLoop error");
        if (errno == EPIPE || errno == ECONNRESET) {
          faultError = true;
        }
      }
    }
  }

  if (!faultError && remaining > 0) {
    outputBuffer_.append(static_cast<const char*>(data) + nwrote, remaining);
    if (!channel_->isWriting()) {
      channel_->enableWriting();
    }
  }
}

/// 做什么：置 kDisconnecting 并在 IO 线程 shutdownInLoop。
/// 项目角色：Codec 发现非法包长时关闭连接。
void TcpConnection::shutdown() {
  if (state_ == kConnected) {
    setState(kDisconnecting);
    loop_->runInLoop([this] { shutdownInLoop(); });
  }
}

/// 做什么：无待发数据时 shutdown(SHUT_WR)。
/// 项目角色：TCP 半关闭，等待读端 EOF。
void TcpConnection::shutdownInLoop() {
  loop_->assertInLoopThread();
  if (!channel_->isWriting()) {
    socket_->shutdownWrite();
  }
}

/// 做什么：queueInLoop forceCloseInLoop。
/// 项目角色：空闲超时 Timer 回调触发。
void TcpConnection::forceClose() {
  if (state_ == kConnected || state_ == kDisconnecting) {
    setState(kDisconnecting);
    loop_->queueInLoop([this] { forceCloseInLoop(); });
  }
}

/// 做什么：disableAll 并 closeCallback_ 通知 TcpServer。
/// 项目角色：连接从 map 移除的触发点之一。
void TcpConnection::forceCloseInLoop() {
  loop_->assertInLoopThread();
  if (state_ == kConnected || state_ == kDisconnecting) {
    setState(kDisconnected);
    channel_->disableAll();
    TcpConnectionPtr guardThis(shared_from_this());
    closeCallback_(guardThis);
  }
}

/// 做什么：enableReading + connectionCallback(connected)。
/// 项目角色：新连接进入 kConnected，chat 在此 add(conn)。
void TcpConnection::connectEstablished() {
  loop_->assertInLoopThread();
  setState(kConnected);
  channel_->enableReading();
  if (connectionCallback_) {
    connectionCallback_(shared_from_this());
  }
}

/// 做什么：cancel idleTimer、disableAll、channel->remove()。
/// 项目角色：TcpServer::removeConnectionInLoop 收尾，释放 epoll 注册。
void TcpConnection::connectDestroyed() {
  loop_->assertInLoopThread();
  if (state_ == kConnected) {
    setState(kDisconnected);
    channel_->disableAll();
    if (idleTimer_) {
      loop_->timerQueue()->cancel(idleTimer_);
      idleTimer_ = nullptr;
    }
  }
  channel_->remove();
}

/// 做什么：ET 循环 readFd；数据交给 messageCallback 或 threadPool。
/// 项目角色：字节流进入 inputBuffer_ → Codec 的入口链。
void TcpConnection::handleRead(Timestamp receiveTime) {
  loop_->assertInLoopThread();
  int savedErrno = 0;
  while (true) {
    ssize_t n = inputBuffer_.readFd(channel_->fd(), &savedErrno);
    if (n > 0) {
      continue;
    }
    if (n == 0) {
      handleClose();
      return;
    }
    if (savedErrno == EAGAIN || savedErrno == EWOULDBLOCK) {
      break;
    }
    handleError();
    return;
  }

  if (inputBuffer_.readableBytes() > 0 && messageCallback_) {
    if (threadPool_) {
      TcpConnectionPtr self(shared_from_this());
      Buffer buf;
      buf.append(inputBuffer_.peek(), inputBuffer_.readableBytes());
      inputBuffer_.retrieveAll();
      threadPool_->run([self, buf = std::move(buf), receiveTime]() mutable {
        self->messageCallback_(self, &buf, receiveTime);
      });
    } else {
      messageCallback_(shared_from_this(), &inputBuffer_, receiveTime);
    }
  }
}

/// 做什么：write outputBuffer_，写空则 disableWriting。
/// 项目角色：POLLOUT 就绪时续传，配合 sendInLoop 缓冲。
void TcpConnection::handleWrite() {
  loop_->assertInLoopThread();
  if (channel_->isWriting()) {
    ssize_t n = ::write(channel_->fd(), outputBuffer_.peek(),
                        outputBuffer_.readableBytes());
    if (n > 0) {
      outputBuffer_.retrieve(n);
      if (outputBuffer_.readableBytes() == 0) {
        channel_->disableWriting();
        if (writeCompleteCallback_) {
          TcpConnectionPtr guardThis(shared_from_this());
          writeCompleteCallback_(guardThis);
        }
        if (state_ == kDisconnecting) {
          shutdownInLoop();
        }
      }
    } else {
      LOG_ERROR("TcpConnection::handleWrite error");
    }
  } else {
    LOG_WARN("Channel is down, no more writing");
  }
}

/// 做什么：置断开并 closeCallback_。
/// 项目角色：客户端断开或 read=0，TcpServer 删连接。
void TcpConnection::handleClose() {
  loop_->assertInLoopThread();
  setState(kDisconnected);
  channel_->disableAll();
  TcpConnectionPtr guardThis(shared_from_this());
  closeCallback_(guardThis);
}

/// 做什么：记录 socket 错误 errno。
/// 项目角色：read/write 非 EAGAIN 错误诊断。
void TcpConnection::handleError() {
  int err = errno;
  LOG_ERROR("TcpConnection::handleError fd=%d err=%d", channel_->fd(), err);
}

/// 做什么：addTimer 周期性检查并 forceClose。
/// 项目角色：TcpServer setConnectionIdleTimeout 落地方案第 10 节超时管理。
void TcpConnection::setIdleTimeout(int seconds) {
  if (seconds <= 0) return;
  Timestamp when(
      Timestamp::now().microSecondsSinceEpoch() +
      seconds * Timestamp::kMicroSecondsPerSecond);
  idleTimer_ = loop_->timerQueue()->addTimer(
      [this] { forceClose(); }, when, seconds * Timestamp::kMicroSecondsPerSecond);
}

}  // namespace reactor
