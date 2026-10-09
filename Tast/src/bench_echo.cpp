/**
 * Echo 压测：对应方案 4.1 短连接/持续请求、2.1 QPS、2.2 延迟
 * 目标服务：echo_server (默认 8080)，Codec 协议
 */
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include "bench_stats.h"
#include "protocol_client.h"

namespace {

struct Config {
  std::string host{"127.0.0.1"};
  int port{8080};
  int threads{4};
  int connsPerThread{50};
  int durationSec{60};
  int msgSize{1024};
  bool shortConn{false};
  uint16_t msgType{1};
};

Config parseArgs(int argc, char** argv) {
  Config c;
  for (int i = 1; i < argc; ++i) {
    if (std::strcmp(argv[i], "--host") == 0 && i + 1 < argc) c.host = argv[++i];
    else if (std::strcmp(argv[i], "--port") == 0 && i + 1 < argc)
      c.port = std::atoi(argv[++i]);
    else if (std::strcmp(argv[i], "--threads") == 0 && i + 1 < argc)
      c.threads = std::atoi(argv[++i]);
    else if (std::strcmp(argv[i], "--conns") == 0 && i + 1 < argc)
      c.connsPerThread = std::atoi(argv[++i]);
    else if (std::strcmp(argv[i], "--duration") == 0 && i + 1 < argc)
      c.durationSec = std::atoi(argv[++i]);
    else if (std::strcmp(argv[i], "--size") == 0 && i + 1 < argc)
      c.msgSize = std::atoi(argv[++i]);
    else if (std::strcmp(argv[i], "--short-conn") == 0) c.shortConn = true;
    else if (std::strcmp(argv[i], "--help") == 0) {
      std::cout
          << "Usage: bench_echo [options]\n"
          << "  --host IP          default 127.0.0.1\n"
          << "  --port N           default 8080\n"
          << "  --threads N        client worker threads, default 4\n"
          << "  --conns N          connections per thread, default 50\n"
          << "  --duration SEC     test duration, default 60\n"
          << "  --size BYTES       payload size, default 1024 (方案 4.1 1KB)\n"
          << "  --short-conn       new TCP per request (短连接场景)\n";
      std::exit(0);
    }
  }
  return c;
}

int connectServer(const Config& cfg) {
  struct sockaddr_in addr {};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(static_cast<uint16_t>(cfg.port));
  if (::inet_pton(AF_INET, cfg.host.c_str(), &addr.sin_addr) <= 0) return -1;
  int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
  if (fd < 0) return -1;
  int yes = 1;
  ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof yes);
  if (::connect(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof addr) < 0) {
    ::close(fd);
    return -1;
  }
  return fd;
}

bool oneRoundTrip(int fd, const std::string& body, uint16_t type,
                  tast::LatencyCollector* lat) {
  using clock = std::chrono::steady_clock;
  auto t0 = clock::now();
  if (!tast::sendFrame(fd, type, body)) return false;
  uint16_t rtype = 0;
  std::string rbody;
  if (!tast::recvFrame(fd, &rtype, &rbody)) return false;
  auto t1 = clock::now();
  if (rbody != body) return false;
  if (lat) {
    double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
    lat->add(ms);
  }
  (void)rtype;
  return true;
}

void workerShort(const Config& cfg, std::atomic<uint64_t>* ok,
                 std::atomic<uint64_t>* fail, tast::LatencyCollector* lat,
                 const std::chrono::steady_clock::time_point& deadline) {
  std::string body(static_cast<size_t>(cfg.msgSize), 'x');
  while (std::chrono::steady_clock::now() < deadline) {
    int fd = connectServer(cfg);
    if (fd < 0) {
      fail->fetch_add(1, std::memory_order_relaxed);
      continue;
    }
    if (oneRoundTrip(fd, body, cfg.msgType, lat)) {
      ok->fetch_add(1, std::memory_order_relaxed);
    } else {
      fail->fetch_add(1, std::memory_order_relaxed);
    }
    ::close(fd);
  }
}

void workerKeep(const Config& cfg, std::atomic<uint64_t>* ok,
                std::atomic<uint64_t>* fail, tast::LatencyCollector* lat,
                const std::chrono::steady_clock::time_point& deadline) {
  std::string body(static_cast<size_t>(cfg.msgSize), 'x');
  std::vector<int> fds(static_cast<size_t>(cfg.connsPerThread), -1);
  for (int& fd : fds) {
    fd = connectServer(cfg);
    if (fd < 0) fail->fetch_add(1, std::memory_order_relaxed);
  }
  while (std::chrono::steady_clock::now() < deadline) {
    for (int& fd : fds) {
      if (std::chrono::steady_clock::now() >= deadline) break;
      if (fd < 0) {
        fd = connectServer(cfg);
        if (fd < 0) {
          fail->fetch_add(1, std::memory_order_relaxed);
          continue;
        }
      }
      if (oneRoundTrip(fd, body, cfg.msgType, lat)) {
        ok->fetch_add(1, std::memory_order_relaxed);
      } else {
        fail->fetch_add(1, std::memory_order_relaxed);
        ::close(fd);
        fd = -1;
      }
    }
  }
  for (int fd : fds) {
    if (fd >= 0) ::close(fd);
  }
}

void worker(const Config& cfg, std::atomic<uint64_t>* ok, std::atomic<uint64_t>* fail,
            tast::LatencyCollector* lat, const std::chrono::steady_clock::time_point& deadline) {
  if (cfg.shortConn) {
    workerShort(cfg, ok, fail, lat, deadline);
  } else {
    workerKeep(cfg, ok, fail, lat, deadline);
  }
}

}  // namespace

int main(int argc, char** argv) {
  Config cfg = parseArgs(argc, argv);
  std::printf("bench_echo -> %s:%d threads=%d conns/thread=%d duration=%ds size=%d %s\n",
              cfg.host.c_str(), cfg.port, cfg.threads, cfg.connsPerThread, cfg.durationSec,
              cfg.msgSize, cfg.shortConn ? "SHORT-CONN" : "KEEP-CONN");

  std::atomic<uint64_t> ok{0}, fail{0};
  tast::LatencyCollector lat;
  auto t0 = std::chrono::steady_clock::now();
  auto deadline = t0 + std::chrono::seconds(cfg.durationSec);

  std::vector<std::thread> threads;
  std::vector<tast::LatencyCollector> perThread(static_cast<size_t>(cfg.threads));
  for (int i = 0; i < cfg.threads; ++i) {
    threads.emplace_back([&cfg, &ok, &fail, &perThread, i, deadline] {
      worker(cfg, &ok, &fail, &perThread[static_cast<size_t>(i)], deadline);
    });
  }
  for (auto& t : threads) t.join();
  auto t1 = std::chrono::steady_clock::now();
  for (auto& c : perThread) lat.merge(c);

  tast::BenchResult r;
  r.ok = ok.load();
  r.fail = fail.load();
  r.elapsedSec = std::chrono::duration<double>(t1 - t0).count();
  tast::printSummary("bench_echo", r, static_cast<size_t>(cfg.msgSize));
  lat.report("bench_echo");
  return r.fail > 0 ? 1 : 0;
}
