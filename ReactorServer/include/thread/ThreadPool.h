#pragma once

#include <functional>
#include <memory>
#include <vector>

#include "base/NonCopyable.h"
#include "base/Thread.h"
#include "thread/TaskQueue.h"

namespace reactor {

/**
 * @class ThreadPool
 * 含义：固定数量 worker 线程 + 共享任务队列。
 * 项目角色：TcpServer 的 workerPool_，将 messageCallback 从 Reactor IO 线程
 *           剥离到业务线程，实现方案第 7 节「网络 IO 与业务计算解耦」。
 */
class ThreadPool : NonCopyable {
 public:
  using Task = std::function<void()>;

  /// 含义：创建池（未启动）。角色：TcpServer 成员 workerPool_ 构造。
  explicit ThreadPool(const std::string& name = "ThreadPool");
  ~ThreadPool();

  /// 含义：预留设置线程数（当前由 start 参数决定）。角色：扩展配置接口。
  void setThreadNum(int numThreads);
  /// 含义：启动 numThreads 个 worker。角色：TcpServer::start 时调用。
  void start(int numThreads);
  /// 含义：stop 队列并 join 所有线程。角色：进程退出或析构。
  void stop();

  /// 含义：提交任务（线程安全）。角色：TcpConnection 投递 message 处理。
  void run(Task task);

  /// 含义：队列中待执行任务数。角色：监控与测试。
  size_t taskCount() const { return queue_.size(); }

 private:
  /// 含义：每个 worker 的主循环。角色：pop 任务并执行。
  void workerLoop();

  std::string name_;                              ///< 含义：线程名前缀。角色：Worker0、Worker1…
  TaskQueue<Task> queue_;                         ///< 含义：任务队列。角色：run 与 worker 之间桥梁。
  std::vector<std::unique_ptr<Thread>> threads_;  ///< 含义：工作线程对象。角色：并发执行载体。
  bool running_;                                  ///< 含义：池是否已启动。角色：workerLoop 退出条件。
};

}  // namespace reactor
