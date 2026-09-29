#pragma once

#include <atomic>
#include <map>
#include <memory>
#include <string>

#include "base/NonCopyable.h"
#include "net/Acceptor.h"
#include "net/InetAddress.h"
#include "net/TcpConnection.h"
#include "thread/ThreadPool.h"

namespace reactor {

class EventLoop;

/// TCP 服务端：Acceptor + 连接表 + 业务线程池
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

  EventLoop* loop_;                    ///< IO 事件循环
  const std::string name_;             ///< 服务名
  Acceptor acceptor_;                  ///< 监听器
  std::unique_ptr<ThreadPool> threadPool_;   ///< 预留 IO 线程池（Main-Sub）
  std::unique_ptr<ThreadPool> workerPool_;   ///< 业务工作线程池
  ConnectionCallback connectionCallback_;
  MessageCallback messageCallback_;
  WriteCompleteCallback writeCompleteCallback_;
  std::atomic<int> started_;           ///< 是否已 start（0/1）
  int nextConnId_;                     ///< 连接自增 id
  int idleTimeoutSeconds_;             ///< 连接空闲超时秒数，0 表示不启用
  int workerThreadNum_;                ///< worker 线程数，0 表示业务在 IO 线程

  using ConnectionMap = std::map<std::string, TcpConnectionPtr>;
  ConnectionMap connections_;          ///< 连接名 -> TcpConnection
};

}  // namespace reactor
