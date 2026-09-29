#pragma once

#include <cstdint>
#include <string>

namespace reactor {

class Timestamp {
 public:
  Timestamp() : microSecondsSinceEpoch_(0) {}
  explicit Timestamp(int64_t microSecondsSinceEpoch)
      : microSecondsSinceEpoch_(microSecondsSinceEpoch) {}

  static Timestamp now();
  std::string toString() const;

  int64_t microSecondsSinceEpoch() const { return microSecondsSinceEpoch_; }

  bool valid() const { return microSecondsSinceEpoch_ > 0; }

  static const int kMicroSecondsPerSecond = 1000 * 1000;

 private:
  int64_t microSecondsSinceEpoch_;
};

inline bool operator<(Timestamp lhs, Timestamp rhs) {
  return lhs.microSecondsSinceEpoch() < rhs.microSecondsSinceEpoch();
}

inline bool operator==(Timestamp lhs, Timestamp rhs) {
  return lhs.microSecondsSinceEpoch() == rhs.microSecondsSinceEpoch();
}

}  // namespace reactor
