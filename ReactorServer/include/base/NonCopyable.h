#pragma once

namespace reactor {

/**
 * @class NonCopyable
 * 含义：禁止对象被拷贝或赋值的 mixin 基类。
 * 项目角色：约束 EventLoop、ThreadPool、Socket 等持有唯一资源的对象，
 *           避免误拷贝导致重复 close fd、双析构等问题。
 */
class NonCopyable {
 public:
  NonCopyable(const NonCopyable&) = delete;
  NonCopyable& operator=(const NonCopyable&) = delete;

 protected:
  /// 含义：允许派生类默认构造。角色：仅派生类可实例化。
  NonCopyable() = default;
  /// 含义：允许派生类默认析构。角色：与 RAII 资源释放配合。
  ~NonCopyable() = default;
};

}  // namespace reactor
