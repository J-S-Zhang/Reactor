#pragma once

#include <memory>
#include <string>

#include "base/NonCopyable.h"
#include "base/Thread.h"
#include "thread/TaskQueue.h"

namespace reactor {

/// 含义：日志严重程度枚举。角色：过滤与格式化输出级别。
enum LogLevel { TRACE, DEBUG, INFO, WARN, ERROR, FATAL };

/**
 * @class AsyncLogger
 * 含义：异步日志单例，前台格式化、后台写盘/控制台。
 * 项目角色：全框架诊断入口（LOG_INFO 等宏），避免同步 IO 阻塞 Reactor IO 线程，
 *           与 Thread + TaskQueue 组成「日志子系统」。
 */
class AsyncLogger : NonCopyable {
 public:
  /// 含义：全局唯一实例。角色：宏 LOG_* 的访问点。
  static AsyncLogger& instance();

  /// 含义：指定日志文件路径（追加写）。角色：运维落盘，空则仅 cout。
  void setLogFile(const std::string& filename);
  /// 含义：设置最低输出级别。角色：生产环境降噪。
  void setLogLevel(LogLevel level);
  /// 含义：格式化一条日志并入队。角色：任意线程可调用，不阻塞 IO。
  void log(LogLevel level, const char* file, int line, const char* fmt, ...);

 private:
  AsyncLogger();
  ~AsyncLogger();
  /// 含义：后台线程循环 pop 队列并输出。角色：消费者侧主逻辑。
  void backendLoop();

  LogLevel level_;                       ///< 含义：级别阈值。角色：log() 内过滤。
  std::string logFile_;                  ///< 含义：文件路径。角色：backend 打开 ofstream。
  TaskQueue<std::string> queue_;         ///< 含义：日志行队列。角色：生产者-消费者缓冲。
  std::unique_ptr<Thread> backendThread_;  ///< 含义：写日志专用线程。角色：与 IO 解耦。
  bool running_;                         ///< 含义：后台是否运行。角色：析构时退出循环。
};

#define LOG_TRACE(fmt, ...) \
  reactor::AsyncLogger::instance().log(reactor::TRACE, __FILE__, __LINE__, fmt, ##__VA_ARGS__)
#define LOG_DEBUG(fmt, ...) \
  reactor::AsyncLogger::instance().log(reactor::DEBUG, __FILE__, __LINE__, fmt, ##__VA_ARGS__)
#define LOG_INFO(fmt, ...) \
  reactor::AsyncLogger::instance().log(reactor::INFO, __FILE__, __LINE__, fmt, ##__VA_ARGS__)
#define LOG_WARN(fmt, ...) \
  reactor::AsyncLogger::instance().log(reactor::WARN, __FILE__, __LINE__, fmt, ##__VA_ARGS__)
#define LOG_ERROR(fmt, ...) \
  reactor::AsyncLogger::instance().log(reactor::ERROR, __FILE__, __LINE__, fmt, ##__VA_ARGS__)
#define LOG_FATAL(fmt, ...) \
  reactor::AsyncLogger::instance().log(reactor::FATAL, __FILE__, __LINE__, fmt, ##__VA_ARGS__)

}  // namespace reactor
