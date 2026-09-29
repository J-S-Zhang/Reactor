#pragma once

#include <functional>
#include <list>
#include <vector>

#include "base/NonCopyable.h"

namespace reactor {

/// 简单时间轮：粗粒度（如秒级）延迟任务，tick 推进槽位
class TimeWheel : NonCopyable {
 public:
  using Task = std::function<void()>;

  explicit TimeWheel(size_t slots = 60);

  void tick();
  void addTask(Task task, size_t delaySlots);

 private:
  size_t currentSlot_;                 ///< 当前槽位下标
  std::vector<std::list<Task>> slots_; ///< 每个槽位上的任务链表
};

}  // namespace reactor
