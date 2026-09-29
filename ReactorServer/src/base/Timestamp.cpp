#include "base/Timestamp.h"

#include <chrono>
#include <iomanip>
#include <sstream>

namespace reactor {

/// 做什么：读取 system_clock 并转为微秒 Timestamp。
/// 项目角色：全框架获取「当前时间」的统一入口。
Timestamp Timestamp::now() {
  using namespace std::chrono;
  auto now = system_clock::now();
  auto micros = duration_cast<microseconds>(now.time_since_epoch()).count();
  return Timestamp(micros);
}

/// 做什么：格式化为 "秒.微秒" 字符串。
/// 项目角色：AsyncLogger 每行前缀时间。
std::string Timestamp::toString() const {
  char buf[32];
  int64_t seconds = microSecondsSinceEpoch_ / kMicroSecondsPerSecond;
  int64_t micros = microSecondsSinceEpoch_ % kMicroSecondsPerSecond;
  std::snprintf(buf, sizeof(buf), "%lld.%06lld",
                static_cast<long long>(seconds),
                static_cast<long long>(micros));
  return buf;
}

}  // namespace reactor
