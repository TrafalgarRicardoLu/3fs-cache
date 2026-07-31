#include "cache_manager/access/AccessFlushWorker.h"

#include <folly/experimental/coro/BlockingWait.h>
#include <folly/logging/xlog.h>

namespace hf3fs::cache_manager {

Result<Void> AccessFlushWorker::Config::valid() const {
  if (flushThreshold == 0 || batchSize == 0 || maxEntries == 0 || flushInterval <= 0_ns ||
      flushThreshold > maxEntries || batchSize > cache::kMaxPhase2BatchItems) {
    return makeError(StatusCode::kInvalidConfig, "invalid cache access flush configuration");
  }
  return Void{};
}

AccessFlushWorker::AccessFlushWorker(std::shared_ptr<CacheManagerBackend> backend,
                                     Config config,
                                     WallClockNsFn wallClockNs)
    : backend_(std::move(backend)),
      config_(config),
      wallClockNs_(std::move(wallClockNs)),
      aggregator_(config.maxEntries) {}

AccessFlushWorker::~AccessFlushWorker() { stop(); }

Result<Void> AccessFlushWorker::start() {
  RETURN_ON_ERROR(config_.valid());
  if (!backend_) return makeError(StatusCode::kInvalidConfig, "cache access backend is not configured");
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

void AccessFlushWorker::stop() {
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

uint64_t AccessFlushWorker::wallClockNs() { return static_cast<uint64_t>(UtcClock::now().toMicroseconds()) * 1000; }

std::vector<AccessReportStatus> AccessFlushWorker::submit(std::span<const CacheAccessReportItem> items) {
  std::vector<AccessReportStatus> results;
  results.reserve(items.size());
  auto lock = std::unique_lock(mutex_);
  if (!running_ || stopping_) {
    results.resize(items.size(), AccessReportStatus::DROPPED);
    dropped_.fetch_add(items.size(), std::memory_order_relaxed);
    return results;
  }
  const auto receiveTimeNs = wallClockNs_ ? wallClockNs_() : wallClockNs();
  for (const auto &item : items) {
    auto accepted = aggregator_.record(item, receiveTimeNs);
    results.push_back(accepted ? AccessReportStatus::ACCEPTED : AccessReportStatus::DROPPED);
    if (!accepted) dropped_.fetch_add(1, std::memory_order_relaxed);
  }
  const auto flush = aggregator_.size() >= config_.flushThreshold;
  lock.unlock();
  if (flush) wake_.notify_one();
  return results;
}

void AccessFlushWorker::flush() {
  auto batch = aggregator_.take(config_.batchSize);
  if (batch.empty()) return;
  const auto batchSize = batch.size();
  auto response = folly::coro::blockingWait(backend_->updateAccess(std::move(batch)));
  if (response.hasError() || response->results.size() != batchSize) {
    failedBatches_.fetch_add(1, std::memory_order_relaxed);
    dropped_.fetch_add(batchSize, std::memory_order_relaxed);
    if (response.hasError()) XLOGF(WARN, "Cache access metadata flush failed: {}", response.error().describe());
    return;
  }
  for (const auto &result : response->results) {
    if (result.hasError()) {
      dropped_.fetch_add(1, std::memory_order_relaxed);
    } else if (!result->updated) {
      stale_.fetch_add(1, std::memory_order_relaxed);
    } else {
      flushed_.fetch_add(1, std::memory_order_relaxed);
    }
  }
}

void AccessFlushWorker::run() {
  auto deadline = std::chrono::steady_clock::now() + config_.flushInterval.asUs();
  while (true) {
    {
      auto lock = std::unique_lock(mutex_);
      wake_.wait_until(lock, deadline, [&] { return stopping_ || aggregator_.size() >= config_.flushThreshold; });
      if (stopping_ && aggregator_.size() == 0) break;
    }
    flush();
    deadline = std::chrono::steady_clock::now() + config_.flushInterval.asUs();
  }
}

AccessFlushWorker::Stats AccessFlushWorker::stats() const {
  return {flushed_.load(std::memory_order_relaxed),
          stale_.load(std::memory_order_relaxed),
          dropped_.load(std::memory_order_relaxed),
          failedBatches_.load(std::memory_order_relaxed)};
}

}  // namespace hf3fs::cache_manager
