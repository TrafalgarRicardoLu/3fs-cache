#pragma once

#include <atomic>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>

#include "cache_manager/access/AccessAggregator.h"
#include "cache_manager/loader/CacheLoader.h"

namespace hf3fs::cache_manager {

class AccessFlushWorker {
 public:
  struct Config {
    size_t flushThreshold{256};
    size_t batchSize{512};
    size_t maxEntries{65536};
    Duration flushInterval{1_s};

    Result<Void> valid() const;
  };

  struct Stats {
    uint64_t flushed{0};
    uint64_t stale{0};
    uint64_t dropped{0};
    uint64_t failedBatches{0};
  };

  using WallClockNsFn = std::function<uint64_t()>;

  AccessFlushWorker(std::shared_ptr<CacheManagerBackend> backend, Config config, WallClockNsFn wallClockNs = {});
  ~AccessFlushWorker();

  Result<Void> start();
  void stop();
  std::vector<AccessReportStatus> submit(std::span<const CacheAccessReportItem> items);

  size_t pending() const { return aggregator_.size(); }
  AccessAggregator::Stats aggregatorStats() const { return aggregator_.stats(); }
  Stats stats() const;

 private:
  static uint64_t wallClockNs();
  void run();
  void flush();

  std::shared_ptr<CacheManagerBackend> backend_;
  Config config_;
  WallClockNsFn wallClockNs_;
  AccessAggregator aggregator_;
  mutable std::mutex mutex_;
  std::condition_variable wake_;
  std::thread worker_;
  bool running_{false};
  bool stopping_{false};
  std::atomic<uint64_t> flushed_{0};
  std::atomic<uint64_t> stale_{0};
  std::atomic<uint64_t> dropped_{0};
  std::atomic<uint64_t> failedBatches_{0};
};

}  // namespace hf3fs::cache_manager
