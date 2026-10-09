/**
 * 并发连接压测：对应方案 2.3 / 4.1 连接数档位 (1000/5000/10000...)
 * 只建立 TCP 并保持，统计连接成功率
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
#include <sys/socket.h>
#include <unistd.h>

namespace {

struct Config {
  std::string host{"127.0.0.1"};
  int port{8080};
  int targetConns{1000};
  int holdSec{60};
  int step{100};  // 每批尝试建立 step 条，便于观察进度
};

Config parseArgs(int argc, char** argv) {
  Config c;
  for (int i = 1; i < argc; ++i) {
    if (std::strcmp(argv[i], "--host") == 0 && i + 1 < argc) c.host = argv[++i];
    else if (std::strcmp(argv[i], "--port") == 0 && i + 1 < argc)
      c.port = std::atoi(argv[++i]);
    else if (std::strcmp(argv[i], "--conns") == 0 && i + 1 < argc)
      c.targetConns = std::atoi(argv[++i]);
    else if (std::strcmp(argv[i], "--hold") == 0 && i + 1 < argc)
      c.holdSec = std::atoi(argv[++i]);
    else if (std::strcmp(argv[i], "--help") == 0) {
      std::cout << "Usage: bench_connect [--host IP] [--port N] [--conns N] [--hold SEC]\n"
                << "  default: 1000 conns, hold 60s\n";
      std::exit(0);
    }
  }
  return c;
}

int tryConnect(const Config& cfg) {
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
  std::printf("bench_connect -> %s:%d target=%d hold=%ds\n", cfg.host.c_str(), cfg.port,
              cfg.targetConns, cfg.holdSec);

  std::vector<int> fds;
  fds.reserve(static_cast<size_t>(cfg.targetConns));
  int fail = 0;
  auto t0 = std::chrono::steady_clock::now();
  for (int i = 0; i < cfg.targetConns; ++i) {
    int fd = tryConnect(cfg);
    if (fd >= 0) {
      fds.push_back(fd);
    } else {
      ++fail;
    }
    if ((i + 1) % cfg.step == 0) {
      std::printf("  progress: ok=%zu fail=%d\n", fds.size(), fail);
    }
  }
  double connectSec =
      std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
  std::printf("connect phase: success=%zu fail=%d time=%.2fs rate=%.0f conn/s\n",
              fds.size(), fail, connectSec,
              connectSec > 0 ? fds.size() / connectSec : 0);

  if (cfg.holdSec > 0 && !fds.empty()) {
    std::printf("holding %zu connections for %d seconds...\n", fds.size(), cfg.holdSec);
    std::this_thread::sleep_for(std::chrono::seconds(cfg.holdSec));
  }

  for (int fd : fds) ::close(fd);
  std::printf("done.\n");
  return fail > 0 ? 1 : 0;
}
