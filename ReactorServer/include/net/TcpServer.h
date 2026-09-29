#pragma once

#include <atomic>
#include <map>
#include <string>

#include "base/NonCopyable.h"
#include "net/Acceptor.h"
#include "net/InetAddress.h"
#include "net/TcpConnection.h"
#include "thread/ThreadPool.h"

namespace reactor {

class EventLoop;

class TcpServer : NonCopyable {
 public:
  using ConnectionCallback = TcpConnection::ConnectionCallback;
  using MessageCallback = TcpConnection::MessageCallback;
  using WriteCompleteCallback = TcpConnection::WriteCompleteCallback;

  TcpServer(EventLoop* loop, const InetAddress& listenAddr,
            const std::string& nameArg);
  ~TcpServer();

  void setThreadNum(int numThreads);
  void setWorkerThreadNum(int numWorkers) { workerThreadNum_ = numWorkers; }
  void start();

  void setConnectionCallback(ConnectionCallback cb) {
    connectionCallback_ = std::move(cb);
  }
  void setMessageCallback(MessageCallback cb) {
    messageCallback_ = std::move(cb);
  }
  void setWriteCompleteCallback(WriteCompleteCallback cb) {
    writeCompleteCallback_ = std::move(cb);
  }

  void setConnectionIdleTimeout(int seconds) { idleTimeoutSeconds_ = seconds; }

  EventLoop* getLoop() const { return loop_; }
  const std::string& name() const { return name_; }

 private:
  void newConnection(int sockfd, const InetAddress& peerAddr);
  void removeConnection(const TcpConnectionPtr& conn);
  void removeConnectionInLoop(const TcpConnectionPtr& conn);

  EventLoop* loop_;
  const std::string name_;
  Acceptor acceptor_;
  std::unique_ptr<ThreadPool> threadPool_;
  std::unique_ptr<ThreadPool> workerPool_;
  ConnectionCallback connectionCallback_;
  MessageCallback messageCallback_;
  WriteCompleteCallback writeCompleteCallback_;
  std::atomic<int> started_;
  int nextConnId_;
  int idleTimeoutSeconds_;
  int workerThreadNum_;

  using ConnectionMap = std::map<std::string, TcpConnectionPtr>;
  ConnectionMap connections_;
};

}  // namespace reactor
