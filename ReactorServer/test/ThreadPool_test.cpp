/// ThreadPool 并发投递 10 个任务
#include <atomic>
#include <chrono>
#include <iostream>
#include <thread>

#include "thread/ThreadPool.h"

int main() {
  reactor::ThreadPool pool("TestPool");
  pool.start(2);
  std::atomic<int> count{0};
  for (int i = 0; i < 10; ++i) {
    pool.run([&count] { count.fetch_add(1); });
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(500));
  assert(count.load() == 10);
  pool.stop();
  std::cout << "ThreadPool_test passed\n";
  return 0;
}
