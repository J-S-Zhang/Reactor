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

/**
 * @class TcpServer
 * 含义：对外 TCP 服务端 API：监听、连接表、回调、业务线程池。
 * 项目角色：应用入口（echo/chat 的 server 对象）；串联 Acceptor + TcpConnection
 *           + ThreadPool，是方案「TCP 长连接管理 + 线程池调度」的Facade。
 */
class TcpServer : NonCopyable {
 public:
  using ConnectionCallback = TcpConnection::ConnectionCallback;
  using MessageCallback = TcpConnection::MessageCallback;
  using WriteCompleteCallback = TcpConnection::WriteCompleteCallback;

  TcpServer(EventLoop* loop, const InetAddress& listenAddr,
            const std::string& nameArg);
  ~TcpServer();

  /// 含义：预留多 IO 线程池配置。角色：Main-Sub Reactor 扩展。
  void setThreadNum(int numThreads);
  /// 含义：业务 worker 数量；0 表示 message 在 IO 线程处理。角色：setWorkerThreadNum(4)。
  void setWorkerThreadNum(int numWorkers) { workerThreadNum_ = numWorkers; }
  /// 含义：启动 worker 与 listen。角色：main 在 loop 前调用。
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

  /// 含义：连接无业务数据超时秒数。角色：防僵死连接。
  void setConnectionIdleTimeout(int seconds) { idleTimeoutSeconds_ = seconds; }

  EventLoop* getLoop() const { return loop_; }
  const std::string& name() const { return name_; }

 private:
  void newConnection(int sockfd, const InetAddress& peerAddr);
  void removeConnection(const TcpConnectionPtr& conn);
  void removeConnectionInLoop(const TcpConnectionPtr& conn);

  EventLoop* loop_;                    ///< 含义：主 IO EventLoop。角色：Acceptor 与所有 Connection 所在 loop。
  const std::string name_;             ///< 含义：服务实例名。角色：连接名前缀、日志。
  Acceptor acceptor_;                  ///< 含义：监听器。角色：accept 新 fd。
  std::unique_ptr<ThreadPool> threadPool_;   ///< 含义：IO 线程池（预留）。角色：暂未使用。
  std::unique_ptr<ThreadPool> workerPool_;   ///< 含义：业务线程池。角色：messageCallback offload。
  ConnectionCallback connectionCallback_;    ///< 含义：用户连接事件。角色：chat 加入/离开。
  MessageCallback messageCallback_;          ///< 含义：用户消息事件。角色：Codec 入口。
  WriteCompleteCallback writeCompleteCallback_; ///< 含义：写完成。角色：用户可选。
  std::atomic<int> started_;           ///< 含义：0/1 是否已 start。角色：防止重复 listen。
  int nextConnId_;                     ///< 含义：连接递增 id。角色：生成 connection name。
  int idleTimeoutSeconds_;             ///< 含义：空闲超时配置。角色：传给 setIdleTimeout。
  int workerThreadNum_;                ///< 含义：worker 线程数。角色：start 时 workerPool_->start。

  using ConnectionMap = std::map<std::string, TcpConnectionPtr>;
  ConnectionMap connections_;          ///< 含义：活跃连接表。角色：生命周期与广播（chat 另维护 set）。
};

}  // namespace reactor
