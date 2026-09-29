#include "base/Timestamp.h"

#include <chrono>
#include <iomanip>
#include <sstream>

namespace reactor {

/// 获取当前系统时间的 Timestamp
Timestamp Timestamp::now() {
  using namespace std::chrono;
  auto now = system_clock::now();
  auto micros = duration_cast<microseconds>(now.time_since_epoch()).count();
  return Timestamp(micros);
}

/// 格式化为 "秒.微秒" 字符串
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
