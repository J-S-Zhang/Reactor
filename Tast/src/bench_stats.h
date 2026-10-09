#pragma once

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <mutex>
#include <vector>

namespace tast {

struct BenchResult {
  uint64_t ok{0};
  uint64_t fail{0};
  double elapsedSec{0};
  std::vector<double> latenciesMs;

  double qps() const {
    return elapsedSec > 0 ? static_cast<double>(ok) / elapsedSec : 0;
  }

  double throughputMBps(size_t bytesPerMsg) const {
    if (elapsedSec <= 0) return 0;
    double bytes = static_cast<double>(ok) * bytesPerMsg * 2;  // req + echo
    return bytes / elapsedSec / (1024.0 * 1024.0);
  }
};

class LatencyCollector {
 public:
  void add(double ms) {
    std::lock_guard<std::mutex> lock(mutex_);
    samples_.push_back(ms);
  }

  void merge(LatencyCollector& other) {
    std::lock_guard<std::mutex> lock(mutex_);
    std::lock_guard<std::mutex> lock2(other.mutex_);
    samples_.insert(samples_.end(), other.samples_.begin(), other.samples_.end());
  }

  void report(const char* title) const {
    std::vector<double> v;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      v = samples_;
    }
    if (v.empty()) {
      std::printf("%s: (no latency samples)\n", title);
      return;
    }
    std::sort(v.begin(), v.end());
    double sum = 0;
    for (double x : v) sum += x;
    auto pct = [&](double p) {
      size_t idx = static_cast<size_t>(std::ceil(p * v.size())) - 1;
      if (idx >= v.size()) idx = v.size() - 1;
      return v[idx];
    };
    std::printf("%s latency(ms): avg=%.3f p50=%.3f p99=%.3f max=%.3f (n=%zu)\n",
                title, sum / v.size(), pct(0.50), pct(0.99), v.back(), v.size());
  }

 private:
  mutable std::mutex mutex_;
  std::vector<double> samples_;
};

inline void printSummary(const char* name, const BenchResult& r, size_t payloadBytes) {
  std::printf("\n========== %s ==========\n", name);
  std::printf("OK=%llu FAIL=%llu elapsed=%.2fs QPS=%.0f throughput~=%.2f MB/s (req+resp)\n",
              static_cast<unsigned long long>(r.ok),
              static_cast<unsigned long long>(r.fail), r.elapsedSec, r.qps(),
              r.throughputMBps(payloadBytes));
}

}  // namespace tast
