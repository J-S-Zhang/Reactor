#pragma once

#include <cstdint>
#include <string>

namespace reactor {

/**
 * @class Timestamp
 * 含义：自 Unix 纪元起的微秒时间戳值类型。
 * 项目角色：为日志打时间、Timer 到期比较、Channel 读事件回调携带「收到数据时刻」
 *           提供统一时间表示，贯穿 base / timer / net 模块。
 */
class Timestamp {
 public:
  /// 含义：无效时间戳（0）。角色：默认构造占位。
  Timestamp() : microSecondsSinceEpoch_(0) {}
  /// 含义：用微秒数构造。角色：Timer 计算下一次到期时刻。
  explicit Timestamp(int64_t microSecondsSinceEpoch)
      : microSecondsSinceEpoch_(microSecondsSinceEpoch) {}

  /// 含义：取当前系统时间。角色：日志、定时器、超时检测的「现在」。
  static Timestamp now();
  /// 含义：格式化为可读字符串。角色：AsyncLogger 输出。
  std::string toString() const;

  /// 含义：返回内部微秒值。角色：时间比较与算术。
  int64_t microSecondsSinceEpoch() const { return microSecondsSinceEpoch_; }

  /// 含义：是否为有效非零时间。角色：判断 Timer 是否已设置。
  bool valid() const { return microSecondsSinceEpoch_ > 0; }

  /// 含义：一秒包含的微秒数。角色：秒↔微秒换算（空闲超时、定时器间隔）。
  static const int kMicroSecondsPerSecond = 1000 * 1000;

 private:
  int64_t microSecondsSinceEpoch_;  ///< 含义：UTC 微秒计数。角色：唯一时间状态。
};

/// 含义：时间先后比较。角色：TimerQueue 按到期时间排序。
inline bool operator<(Timestamp lhs, Timestamp rhs) {
  return lhs.microSecondsSinceEpoch() < rhs.microSecondsSinceEpoch();
}

/// 含义：时间相等比较。角色：测试与断言。
inline bool operator==(Timestamp lhs, Timestamp rhs) {
  return lhs.microSecondsSinceEpoch() == rhs.microSecondsSinceEpoch();
}

}  // namespace reactor
