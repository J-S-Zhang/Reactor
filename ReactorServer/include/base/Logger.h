#pragma once

#include <memory>
#include <string>

#include "base/NonCopyable.h"
#include "base/Thread.h"
#include "thread/TaskQueue.h"

namespace reactor {

enum LogLevel { TRACE, DEBUG, INFO, WARN, ERROR, FATAL };

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

  LogLevel level_;
  std::string logFile_;
  TaskQueue<std::string> queue_;
  std::unique_ptr<Thread> backendThread_;
  bool running_;
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
