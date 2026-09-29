#pragma once

#include <functional>
#include <list>
#include <vector>

#include "base/NonCopyable.h"

namespace reactor {

/**
 * @class TimeWheel
 * 含义：槽位数组 + 链表的时间轮（粗粒度延迟调度）。
 * 项目角色：方案中的可选定时实现，适合秒级大量超时；当前为接口预留，
 *           与 TimerQueue（精确定时）互补，可用于后续连接超时优化。
 */
class TimeWheel : NonCopyable {
 public:
  using Task = std::function<void()>;

  /// 含义：创建 slots 个槽位。角色：默认 60 槽≈60 秒一圈（需外部 tick）。
  explicit TimeWheel(size_t slots = 60);

  /// 含义：推进一格并执行当前槽任务。角色：由 1s 定时器驱动。
  void tick();
  /// 含义：delaySlots 格后执行任务。角色：粗粒度 delay。
  void addTask(Task task, size_t delaySlots);

 private:
  size_t currentSlot_;                 ///< 含义：当前槽下标。角色：tick 时递增取模。
  std::vector<std::list<Task>> slots_; ///< 含义：每槽任务链。角色：O(1) 插入。
};

}  // namespace reactor
