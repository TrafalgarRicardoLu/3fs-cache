#include "client/cache/CacheAccessReporter.h"

#include <algorithm>
#include <folly/experimental/coro/BlockingWait.h>
#include <folly/logging/xlog.h>

#include "cache/metrics/CacheMetrics.h"

namespace hf3fs::client::cache {

Result<Void> CacheAccessReporter::Config::valid() const {
  if (flushThreshold == 0 || maxBatchSize == 0 || maxBufferedItems == 0 || flushInterval <= 0_ns ||
      requestTimeout <= 0_ns) {
    return makeError(StatusCode::kInvalidConfig, "invalid cache access reporter configuration");
  }
  if (flushThreshold > maxBufferedItems || maxBatchSize > hf3fs::cache::kMaxPhase2BatchItems) {
    return makeError(StatusCode::kInvalidConfig, "cache access reporter limits are inconsistent");
  }
  return Void{};
}

CacheAccessReporter::CacheAccessReporter(cache_manager::ICacheManagerServiceStub &stub, Config config)
    : CacheAccessReporter(
          [&stub](const cache_manager::ReportCacheAccessReq &request, const net::UserRequestOptions &options) {
            return stub.reportCacheAccess(request, options);
          },
          config) {}

CacheAccessReporter::CacheAccessReporter(SendFn send, Config config)
    : send_(std::move(send)),
      config_(config) {}

CacheAccessReporter::~CacheAccessReporter() { stop(); }

Result<Void> CacheAccessReporter::start() {
  RETURN_ON_ERROR(config_.valid());
  if (!send_) return makeError(StatusCode::kInvalidConfig, "cache access reporter sender is not configured");
  auto lock = std::unique_lock(mutex_);
  if (running_) return Void{};
  stopping_ = false;
  try {
    worker_ = std::thread([this] { run(); });
  } catch (const std::system_error &error) {
    return makeError(StatusCode::kQueueConflict, error.what());
  }
  running_ = true;
  return Void{};
}

void CacheAccessReporter::stop() {
  {
    auto lock = std::unique_lock(mutex_);
    if (!running_) return;
    stopping_ = true;
  }
  wake_.notify_one();
  if (worker_.joinable()) worker_.join();
  auto lock = std::unique_lock(mutex_);
  running_ = false;
}

uint64_t CacheAccessReporter::wallClockNs() { return static_cast<uint64_t>(UtcClock::now().toMicroseconds()) * 1000; }

bool CacheAccessReporter::record(const flat::UserInfo &user,
                                 const hf3fs::cache::CacheBlockKey &key,
                                 hf3fs::cache::CacheGeneration generation,
                                 uint64_t clientObservedTimeNs) {
  cache_manager::CacheAccessReportItem item{key,
                                            generation,
                                            clientObservedTimeNs == 0 ? wallClockNs() : clientObservedTimeNs};
  if (item.valid().hasError()) {
    dropped_.fetch_add(1, std::memory_order_relaxed);
    return false;
  }
  bool flush = false;
  {
    auto lock = std::unique_lock(mutex_);
    if (!running_ || stopping_ || buffer_.size() >= config_.maxBufferedItems) {
      dropped_.fetch_add(1, std::memory_order_relaxed);
      hf3fs::cache::metrics::recordCount(
          hf3fs::cache::metrics::Event::CLIENT_REPORT_RESULT,
          1,
          {.inode = key.inode, .block = key.block.toUnderType(), .reason = "access_buffer_full"});
      return false;
    }
    buffer_.push_back({user, std::move(item)});
    accepted_.fetch_add(1, std::memory_order_relaxed);
    flush = buffer_.size() >= config_.flushThreshold;
  }
  if (flush) wake_.notify_one();
  return true;
}

std::vector<CacheAccessReporter::Entry> CacheAccessReporter::takeBatch() {
  auto lock = std::unique_lock(mutex_);
  if (buffer_.empty()) return {};
  std::vector<Entry> batch;
  batch.reserve(std::min(config_.maxBatchSize, buffer_.size()));
  auto user = buffer_.front().user;
  for (auto iter = buffer_.begin(); iter != buffer_.end() && batch.size() < config_.maxBatchSize;) {
    if (iter->user == user) {
      batch.push_back(std::move(*iter));
      iter = buffer_.erase(iter);
    } else {
      ++iter;
    }
  }
  return batch;
}

void CacheAccessReporter::send(std::vector<Entry> batch) {
  if (batch.empty()) return;
  cache_manager::ReportCacheAccessReq request;
  request.user = batch.front().user;
  request.items.reserve(batch.size());
  for (auto &entry : batch) request.items.push_back(std::move(entry.item));
  request.cacheProtocolVersion = hf3fs::cache::kCacheProtocolVersion;
  net::UserRequestOptions options;
  options.timeout = config_.requestTimeout;
  options.sendRetryTimes = 0;
  auto response = folly::coro::blockingWait(send_(request, options));
  if (response.hasError() || response->results.size() != request.items.size()) {
    failedBatches_.fetch_add(1, std::memory_order_relaxed);
    dropped_.fetch_add(request.items.size(), std::memory_order_relaxed);
    if (response.hasError()) XLOGF(WARN, "Cache access report failed: {}", response.error().describe());
    return;
  }
  uint64_t delivered = 0;
  for (const auto &result : response->results) {
    if (result.hasValue() && result->status != cache_manager::AccessReportStatus::DROPPED) {
      ++delivered;
    }
  }
  sent_.fetch_add(delivered, std::memory_order_relaxed);
  dropped_.fetch_add(request.items.size() - delivered, std::memory_order_relaxed);
}

void CacheAccessReporter::run() {
  auto deadline = std::chrono::steady_clock::now() + config_.flushInterval.asUs();
  while (true) {
    {
      auto lock = std::unique_lock(mutex_);
      wake_.wait_until(lock, deadline, [&] { return stopping_ || buffer_.size() >= config_.flushThreshold; });
      if (stopping_ && buffer_.empty()) break;
    }
    send(takeBatch());
    deadline = std::chrono::steady_clock::now() + config_.flushInterval.asUs();
  }
}

CacheAccessReporter::Stats CacheAccessReporter::stats() const {
  return {accepted_.load(std::memory_order_relaxed),
          sent_.load(std::memory_order_relaxed),
          dropped_.load(std::memory_order_relaxed),
          failedBatches_.load(std::memory_order_relaxed)};
}

size_t CacheAccessReporter::buffered() const {
  auto lock = std::unique_lock(mutex_);
  return buffer_.size();
}

}  // namespace hf3fs::client::cache
