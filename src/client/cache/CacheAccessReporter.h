#pragma once

#include <atomic>
#include <condition_variable>
#include <deque>
#include <functional>
#include <mutex>
#include <thread>

#include "common/net/RequestOptions.h"
#include "fbs/cache_manager/Service.h"
#include "stubs/cache_manager/ICacheManagerServiceStub.h"

namespace hf3fs::client::cache {

class ICacheAccessReporter {
 public:
  virtual ~ICacheAccessReporter() = default;
  virtual bool record(const flat::UserInfo &user,
                      const hf3fs::cache::CacheBlockKey &key,
                      hf3fs::cache::CacheGeneration generation,
                      uint64_t clientObservedTimeNs = 0) = 0;
};

class CacheAccessReporter final : public ICacheAccessReporter {
 public:
  struct Config {
    size_t flushThreshold{64};
    size_t maxBatchSize{256};
    size_t maxBufferedItems{4096};
    Duration flushInterval{1_s};
    Duration requestTimeout{200_ms};

    Result<Void> valid() const;
  };

  struct Stats {
    uint64_t accepted{0};
    uint64_t sent{0};
    uint64_t dropped{0};
    uint64_t failedBatches{0};
  };

  using SendFn =
      std::function<CoTryTask<cache_manager::ReportCacheAccessRsp>(const cache_manager::ReportCacheAccessReq &,
                                                                   const net::UserRequestOptions &)>;

  CacheAccessReporter(cache_manager::ICacheManagerServiceStub &stub, Config config);
  CacheAccessReporter(SendFn send, Config config);
  ~CacheAccessReporter();

  Result<Void> start();
  void stop();

  bool record(const flat::UserInfo &user,
              const hf3fs::cache::CacheBlockKey &key,
              hf3fs::cache::CacheGeneration generation,
              uint64_t clientObservedTimeNs = 0) final;

  Stats stats() const;
  size_t buffered() const;

 private:
  struct Entry {
    flat::UserInfo user;
    cache_manager::CacheAccessReportItem item;
  };

  static uint64_t wallClockNs();
  void run();
  std::vector<Entry> takeBatch();
  void send(std::vector<Entry> batch);

  SendFn send_;
  Config config_;
  mutable std::mutex mutex_;
  std::condition_variable wake_;
  std::deque<Entry> buffer_;
  std::thread worker_;
  bool running_{false};
  bool stopping_{false};
  std::atomic<uint64_t> accepted_{0};
  std::atomic<uint64_t> sent_{0};
  std::atomic<uint64_t> dropped_{0};
  std::atomic<uint64_t> failedBatches_{0};
};

}  // namespace hf3fs::client::cache
