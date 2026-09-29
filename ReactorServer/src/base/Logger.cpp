#include "base/Logger.h"

#include <cstdarg>
#include <cstdio>
#include <fstream>
#include <iostream>

#include "base/Timestamp.h"

namespace reactor {

namespace {
/// 日志级别转字符串
const char* levelName(LogLevel level) {
  switch (level) {
    case TRACE:
      return "TRACE";
    case DEBUG:
      return "DEBUG";
    case INFO:
      return "INFO";
    case WARN:
      return "WARN";
    case ERROR:
      return "ERROR";
    case FATAL:
      return "FATAL";
    default:
      return "UNKNOWN";
  }
}
}  // namespace

/// 单例访问
AsyncLogger& AsyncLogger::instance() {
  static AsyncLogger logger;
  return logger;
}

/// 启动后台日志线程
AsyncLogger::AsyncLogger() : level_(INFO), running_(true) {
  backendThread_ = std::make_unique<Thread>([this] { backendLoop(); }, "Logger");
  backendThread_->start();
}

/// 停止后台线程并 join
AsyncLogger::~AsyncLogger() {
  running_ = false;
  queue_.stop();
  if (backendThread_) backendThread_->join();
}

/// 设置日志文件路径
void AsyncLogger::setLogFile(const std::string& filename) {
  logFile_ = filename;
}

/// 设置最低输出级别
void AsyncLogger::setLogLevel(LogLevel level) { level_ = level; }

/// 格式化一条日志并入队（可能从任意线程调用）
void AsyncLogger::log(LogLevel level, const char* file, int line, const char* fmt,
                      ...) {
  if (level < level_) return;

  char msgBuf[4096];
  va_list args;
  va_start(args, fmt);
  vsnprintf(msgBuf, sizeof(msgBuf), fmt, args);
  va_end(args);

  std::string lineStr = Timestamp::now().toString();
  lineStr += " [";
  lineStr += levelName(level);
  lineStr += "] ";
  lineStr += file;
  lineStr += ":";
  lineStr += std::to_string(line);
  lineStr += " ";
  lineStr += msgBuf;
  lineStr += "\n";

  queue_.push(std::move(lineStr));
}

/// 后台线程：从队列取日志并写入控制台/文件
void AsyncLogger::backendLoop() {
  std::string item;
  std::ofstream file;
  if (!logFile_.empty()) {
    file.open(logFile_, std::ios::app);
  }
  while (running_ || queue_.size() > 0) {
    if (queue_.pop(item, 200)) {
      std::cout << item;
      if (file.is_open()) file << item;
    }
  }
  if (file.is_open()) file.close();
}

}  // namespace reactor
