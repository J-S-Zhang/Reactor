#pragma once

namespace reactor {

/// 禁止拷贝/赋值的基类，用于 EventLoop、ThreadPool 等不应被复制的资源型对象
class NonCopyable {
 public:
  NonCopyable(const NonCopyable&) = delete;
  NonCopyable& operator=(const NonCopyable&) = delete;

 protected:
  NonCopyable() = default;
  ~NonCopyable() = default;
};

}  // namespace reactor
