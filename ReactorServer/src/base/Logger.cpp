#include "base/Logger.h"

#include <cstdarg>
#include <cstdio>
#include <fstream>
#include <iostream>

#include "base/Timestamp.h"

namespace reactor {

namespace {
/// 做什么：LogLevel 转字符串。
/// 项目角色：日志行 human-readable 级别字段。
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

/// 做什么：返回 Meyers 单例。
/// 项目角色：LOG_* 宏的全局访问点。
AsyncLogger& AsyncLogger::instance() {
  static AsyncLogger logger;
  return logger;
}

/// 做什么：默认 INFO 级别并启动名为 Logger 的后台线程。
/// 项目角色：进程启动后首次 LOG 前自动初始化异步写日志。
AsyncLogger::AsyncLogger() : level_(INFO), running_(true) {
  backendThread_ = std::make_unique<Thread>([this] { backendLoop(); }, "Logger");
  backendThread_->start();
}

/// 做什么：停止后台循环并 join。
/// 项目角色：进程退出前尽量刷完队列（静态析构顺序依赖实现）。
AsyncLogger::~AsyncLogger() {
  running_ = false;
  queue_.stop();
  if (backendThread_) backendThread_->join();
}

/// 做什么：保存日志文件路径。
/// 项目角色：运维配置落盘路径。
void AsyncLogger::setLogFile(const std::string& filename) {
  logFile_ = filename;
}

/// 做什么：设置过滤阈值。
/// 项目角色：生产/调试环境切换 verbosity。
void AsyncLogger::setLogLevel(LogLevel level) { level_ = level; }

/// 做什么：vsnprintf 格式化后经队列异步输出。
/// 项目角色：全项目诊断，任意线程可调用且不阻塞 Reactor。
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

/// 做什么：循环 pop 日志行写 cout/文件。
/// 项目角色：异步日志消费者，与 IO 线程分离磁盘 IO。
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
