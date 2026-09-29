#pragma once

#include <functional>

#include "base/Timestamp.h"

namespace reactor {

/**
 * @class Timer
 * 含义：单个定时任务（到期时刻 + 可选周期 + 回调）。
 * 项目角色：被 TimerQueue 管理；用于连接空闲超时、心跳等方案第 8 节定时能力。
 */
class Timer {
 public:
  using TimerCallback = std::function<void()>;

  /// 含义：构造定时器；interval>0 为重复定时。角色：addTimer 时 heap 分配。
  Timer(TimerCallback cb, Timestamp when, int64_t interval)
      : callback_(std::move(cb)),
        expiration_(when),
        interval_(interval),
        repeat_(interval > 0) {}

  /// 含义：执行用户回调。角色：TimerQueue::handleRead 到期时调用。
  void run() const {
    if (callback_) callback_();
  }

  /// 含义：下次到期时间。角色：TimerQueue 按时间排序。
  Timestamp expiration() const { return expiration_; }
  /// 含义：是否周期性。角色：到期后 delete 或 re-insert。
  bool repeat() const { return repeat_; }
  /// 含义：重复间隔（微秒）。角色：restart 计算下一到期；ActiveTimer 键。
  int64_t interval() const { return interval_; }

  /// 含义：重复定时器推进到下一到期。角色：一次触发后重新入队。
  void restart(Timestamp now);

 private:
  TimerCallback callback_;   ///< 含义：到期动作。角色：如 TcpConnection::forceClose。
  Timestamp expiration_;     ///< 含义：绝对到期时刻。角色：set 排序键 first。
  const int64_t interval_; ///< 含义：周期间隔微秒。角色：0 表示 one-shot。
  const bool repeat_;        ///< 含义：是否重复。角色：由 interval_ 推导。
};

}  // namespace reactor
