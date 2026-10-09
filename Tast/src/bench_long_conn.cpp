/**
 * 长连接 + 周期心跳包：对应方案 4.2
 * 使用 Codec type=1 小 payload 模拟 heartbeat（服务端 echo 即可）
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
  int connections{10000};
  int durationSec{1800};  // 30min
  int intervalMs{5000};
  int payloadSize{16};
};

Config parseArgs(int argc, char** argv) {
  Config c;
  for (int i = 1; i < argc; ++i) {
    if (std::strcmp(argv[i], "--host") == 0 && i + 1 < argc) c.host = argv[++i];
    else if (std::strcmp(argv[i], "--port") == 0 && i + 1 < argc)
      c.port = std::atoi(argv[++i]);
    else if (std::strcmp(argv[i], "--conns") == 0 && i + 1 < argc)
      c.connections = std::atoi(argv[++i]);
    else if (std::strcmp(argv[i], "--duration") == 0 && i + 1 < argc)
      c.durationSec = std::atoi(argv[++i]);
    else if (std::strcmp(argv[i], "--interval") == 0 && i + 1 < argc)
      c.intervalMs = std::atoi(argv[++i]);
    else if (std::strcmp(argv[i], "--help") == 0) {
      std::cout << "Usage: bench_long_conn [--conns N] [--duration SEC] [--interval MS]\n";
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
  if (::connect(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof addr) < 0) {
    ::close(fd);
    return -1;
  }
  return fd;
}

}  // namespace

int main(int argc, char** argv) {
  Config cfg = parseArgs(argc, argv);
  std::printf("bench_long_conn -> %s:%d conns=%d duration=%ds interval=%dms\n",
              cfg.host.c_str(), cfg.port, cfg.connections, cfg.durationSec,
              cfg.intervalMs);

  std::atomic<uint64_t> heartbeats{0}, errors{0};
  std::vector<int> fds;
  int connectFail = 0;
  for (int i = 0; i < cfg.connections; ++i) {
    int fd = connectServer(cfg);
    if (fd >= 0) fds.push_back(fd);
    else ++connectFail;
    if ((i + 1) % 1000 == 0) {
      std::printf("  connected %zu / %d (fail=%d)\n", fds.size(), i + 1, connectFail);
    }
  }

  std::atomic<bool> stop{false};
  std::thread ticker([&] {
    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::seconds(cfg.durationSec);
    std::string body(static_cast<size_t>(cfg.payloadSize), 'h');
    while (!stop.load() && std::chrono::steady_clock::now() < deadline) {
      for (int fd : fds) {
        if (!tast::sendFrame(fd, 1, body)) {
          errors.fetch_add(1, std::memory_order_relaxed);
          continue;
        }
        uint16_t type = 0;
        std::string resp;
        if (tast::recvFrame(fd, &type, &resp) && resp == body) {
          heartbeats.fetch_add(1, std::memory_order_relaxed);
        } else {
          errors.fetch_add(1, std::memory_order_relaxed);
        }
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(cfg.intervalMs));
    }
    stop.store(true);
  });

  ticker.join();
  for (int fd : fds) ::close(fd);

  std::printf("long_conn summary: live_conns=%zu connect_fail=%d heartbeats_ok=%llu errors=%llu\n",
              fds.size(), connectFail,
              static_cast<unsigned long long>(heartbeats.load()),
              static_cast<unsigned long long>(errors.load()));
  return (connectFail > 0 || errors.load() > 0) ? 1 : 0;
}
