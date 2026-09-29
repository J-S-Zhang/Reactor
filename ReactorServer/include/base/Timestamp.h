#pragma once

#include <cstdint>
#include <string>

namespace reactor {

/// 微秒精度时间戳，用于日志、定时器与 IO 事件回调时间
class Timestamp {
 public:
  Timestamp() : microSecondsSinceEpoch_(0) {}
  explicit Timestamp(int64_t microSecondsSinceEpoch)
      : microSecondsSinceEpoch_(microSecondsSinceEpoch) {}

  static Timestamp now();
  std::string toString() const;

  int64_t microSecondsSinceEpoch() const { return microSecondsSinceEpoch_; }

  bool valid() const { return microSecondsSinceEpoch_ > 0; }

  /// 每秒对应的微秒数
  static const int kMicroSecondsPerSecond = 1000 * 1000;

 private:
  /// Unix 纪元起的微秒数
  int64_t microSecondsSinceEpoch_;
};

inline bool operator<(Timestamp lhs, Timestamp rhs) {
  return lhs.microSecondsSinceEpoch() < rhs.microSecondsSinceEpoch();
}

inline bool operator==(Timestamp lhs, Timestamp rhs) {
  return lhs.microSecondsSinceEpoch() == rhs.microSecondsSinceEpoch();
}

}  // namespace reactor
