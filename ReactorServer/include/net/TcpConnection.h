#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <string>

#include "base/NonCopyable.h"
#include "buffer/Buffer.h"
#include "net/InetAddress.h"
#include "timer/TimerQueue.h"

namespace reactor {

class Channel;
class EventLoop;
class Socket;
class ThreadPool;

class TcpConnection : NonCopyable,
                      public std::enable_shared_from_this<TcpConnection> {
 public:
  using TcpConnectionPtr = std::shared_ptr<TcpConnection>;
  using ConnectionCallback =
      std::function<void(const TcpConnectionPtr&)>;
  using MessageCallback =
      std::function<void(const TcpConnectionPtr&, Buffer*, Timestamp)>;
  using CloseCallback = std::function<void(const TcpConnectionPtr&)>;
  using WriteCompleteCallback =
      std::function<void(const TcpConnectionPtr&)>;

  enum StateE { kDisconnected, kConnecting, kConnected, kDisconnecting };

  TcpConnection(EventLoop* loop, const std::string& name, int sockfd,
                const InetAddress& localAddr, const InetAddress& peerAddr);
  ~TcpConnection();

  void send(const std::string& message);
  void send(Buffer* buf);
  void shutdown();
  void forceClose();

  void setConnectionCallback(ConnectionCallback cb) {
    connectionCallback_ = std::move(cb);
  }
  void setMessageCallback(MessageCallback cb) {
    messageCallback_ = std::move(cb);
  }
  void setCloseCallback(CloseCallback cb) { closeCallback_ = std::move(cb); }
  void setWriteCompleteCallback(WriteCompleteCallback cb) {
    writeCompleteCallback_ = std::move(cb);
  }

  void connectEstablished();
  void connectDestroyed();

  EventLoop* getLoop() const { return loop_; }
  const std::string& name() const { return name_; }
  const InetAddress& localAddress() const { return localAddr_; }
  const InetAddress& peerAddress() const { return peerAddr_; }
  bool connected() const { return state_ == kConnected; }

  void setThreadPool(ThreadPool* pool) { threadPool_ = pool; }
  void setIdleTimeout(int seconds);

 private:
  void handleRead(Timestamp receiveTime);
  void handleWrite();
  void handleClose();
  void handleError();
  void sendInLoop(const std::string& message);
  void sendInLoop(const void* data, size_t len);
  void shutdownInLoop();
  void forceCloseInLoop();
  void setState(StateE s) { state_ = s; }

  EventLoop* loop_;
  const std::string name_;
  std::atomic<StateE> state_;
  bool reading_;

  std::unique_ptr<Socket> socket_;
  std::unique_ptr<Channel> channel_;
  const InetAddress localAddr_;
  const InetAddress peerAddr_;

  Buffer inputBuffer_;
  Buffer outputBuffer_;

  ConnectionCallback connectionCallback_;
  MessageCallback messageCallback_;
  CloseCallback closeCallback_;
  WriteCompleteCallback writeCompleteCallback_;

  ThreadPool* threadPool_;
  TimerId idleTimer_;
};

}  // namespace reactor
