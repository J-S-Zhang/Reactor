#pragma once

#include <functional>
#include <list>
#include <vector>

#include "base/NonCopyable.h"

namespace reactor {

// 简单时间轮，可用于粗粒度延迟任务（秒级）
class TimeWheel : NonCopyable {
 public:
  using Task = std::function<void()>;

  explicit TimeWheel(size_t slots = 60);

  void tick();
  void addTask(Task task, size_t delaySlots);

 private:
  size_t currentSlot_;
  std::vector<std::list<Task>> slots_;
};

}  // namespace reactor
