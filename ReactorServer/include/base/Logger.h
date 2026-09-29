#pragma once

#include <memory>
#include <string>

#include "base/NonCopyable.h"
#include "base/Thread.h"
#include "thread/TaskQueue.h"

namespace reactor {

/// 日志级别，数值越大越严重
enum LogLevel { TRACE, DEBUG, INFO, WARN, ERROR, FATAL };

/// 异步日志：前台格式化入队，后台线程写 stdout/文件
class AsyncLogger : NonCopyable {
 public:
  static AsyncLogger& instance();

  void setLogFile(const std::string& filename);
  void setLogLevel(LogLevel level);
  void log(LogLevel level, const char* file, int line, const char* fmt, ...);

 private:
  AsyncLogger();
  ~AsyncLogger();
  void backendLoop();

  LogLevel level_;                      ///< 低于此级别的日志被丢弃
  std::string logFile_;                 ///< 非空则追加写入该文件
  TaskQueue<std::string> queue_;        ///< 待输出的日志行队列
  std::unique_ptr<Thread> backendThread_;  ///< 消费队列的后台线程
  bool running_;                        ///< 后台循环是否继续
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
