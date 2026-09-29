#include "net/TcpConnection.h"

#include <errno.h>
#include <unistd.h>

#include "base/Logger.h"
#include "net/Channel.h"
#include "net/EventLoop.h"
#include "net/Socket.h"
#include "thread/ThreadPool.h"

namespace reactor {

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

TcpConnection::~TcpConnection() {
  LOG_DEBUG("TcpConnection::~TcpConnection() %s", name_.c_str());
}

void TcpConnection::send(const std::string& message) {
  if (state_ == kConnected) {
    if (loop_->isInLoopThread()) {
      sendInLoop(message);
    } else {
      loop_->runInLoop([this, message] { sendInLoop(message); });
    }
  }
}

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

void TcpConnection::sendInLoop(const std::string& message) {
  sendInLoop(message.data(), message.size());
}

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

void TcpConnection::shutdown() {
  if (state_ == kConnected) {
    setState(kDisconnecting);
    loop_->runInLoop([this] { shutdownInLoop(); });
  }
}

void TcpConnection::shutdownInLoop() {
  loop_->assertInLoopThread();
  if (!channel_->isWriting()) {
    socket_->shutdownWrite();
  }
}

void TcpConnection::forceClose() {
  if (state_ == kConnected || state_ == kDisconnecting) {
    setState(kDisconnecting);
    loop_->queueInLoop([this] { forceCloseInLoop(); });
  }
}

void TcpConnection::forceCloseInLoop() {
  loop_->assertInLoopThread();
  if (state_ == kConnected || state_ == kDisconnecting) {
    setState(kDisconnected);
    channel_->disableAll();
    TcpConnectionPtr guardThis(shared_from_this());
    closeCallback_(guardThis);
  }
}

void TcpConnection::connectEstablished() {
  loop_->assertInLoopThread();
  setState(kConnected);
  channel_->enableReading();
  if (connectionCallback_) {
    connectionCallback_(shared_from_this());
  }
}

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

void TcpConnection::handleClose() {
  loop_->assertInLoopThread();
  setState(kDisconnected);
  channel_->disableAll();
  TcpConnectionPtr guardThis(shared_from_this());
  closeCallback_(guardThis);
}

void TcpConnection::handleError() {
  int err = errno;
  LOG_ERROR("TcpConnection::handleError fd=%d err=%d", channel_->fd(), err);
}

void TcpConnection::setIdleTimeout(int seconds) {
  if (seconds <= 0) return;
  Timestamp when(
      Timestamp::now().microSecondsSinceEpoch() +
      seconds * Timestamp::kMicroSecondsPerSecond);
  idleTimer_ = loop_->timerQueue()->addTimer(
      [this] { forceClose(); }, when, seconds * Timestamp::kMicroSecondsPerSecond);
}

}  // namespace reactor
