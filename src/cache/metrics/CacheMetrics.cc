#include "cache/metrics/CacheMetrics.h"

#include <atomic>
#include <limits>
#include <mutex>

#include "common/monitor/Recorder.h"

namespace hf3fs::cache::metrics {
namespace {

constexpr auto kEventCount = static_cast<size_t>(Event::COUNT);
std::array<std::atomic<uint64_t>, kEventCount> counters{};
std::array<Tags, kEventCount> lastTags;
std::mutex tagsMutex;

size_t index(Event event) { return static_cast<size_t>(event); }
bool valid(Event event) { return index(event) < kEventCount; }

monitor::TagSet toTagSet(const Tags &tags) {
  constexpr size_t kMaxTagLength = 64;
  auto bounded = [](const std::string &value) { return value.substr(0, kMaxTagLength); };
  monitor::TagSet result;
  if (tags.inode) result.addTag("inode", std::to_string(*tags.inode));
  if (tags.block) result.addTag("block", std::to_string(*tags.block));
  if (tags.originId) result.addTag("origin_id", std::to_string(*tags.originId));
  if (!tags.diskId.empty()) result.addTag("disk_id", tags.diskId);
  if (!tags.generation.empty()) result.addTag("generation", tags.generation);
  if (!tags.epoch.empty()) result.addTag("epoch", tags.epoch);
  if (!tags.operationId.empty()) result.addTag("operation_id", tags.operationId);
  if (!tags.sourceId.empty()) result.addTag("source_id", bounded(tags.sourceId));
  if (tags.sequence) result.addTag("sequence", std::to_string(*tags.sequence));
  if (!tags.reason.empty()) result.addTag("reason", bounded(tags.reason));
  return result;
}

void remember(Event event, const Tags &tags) {
  auto lock = std::unique_lock(tagsMutex);
  lastTags[index(event)] = tags;
  lastTags[index(event)].sourceId.resize(std::min<size_t>(lastTags[index(event)].sourceId.size(), 64));
  lastTags[index(event)].reason.resize(std::min<size_t>(lastTags[index(event)].reason.size(), 64));
}

monitor::CountRecorder &countRecorder(Event event) {
#define CACHE_COUNT_RECORDER(EVENT, NAME)         \
  case Event::EVENT: {                            \
    static monitor::CountRecorder recorder{NAME}; \
    return recorder;                              \
  }
  switch (event) {
    CACHE_COUNT_RECORDER(CLIENT_HIT_BYTES, "cache.client.hit_bytes");
    CACHE_COUNT_RECORDER(CLIENT_MISS_BYTES, "cache.client.miss_bytes");
    CACHE_COUNT_RECORDER(CLIENT_ORIGIN_BYTES, "cache.client.origin_bytes");
    CACHE_COUNT_RECORDER(CLIENT_HINT_RESULT, "cache.client.hint_result");
    CACHE_COUNT_RECORDER(CLIENT_REPORT_RESULT, "cache.client.report_result");
    CACHE_COUNT_RECORDER(META_STATE_TRANSITION, "cache.meta.state_transition");
    CACHE_COUNT_RECORDER(META_CHARGE_BYTES, "cache.meta.charge_bytes");
    CACHE_COUNT_RECORDER(META_EPOCH_CONFLICT, "cache.meta.epoch_conflict");
    CACHE_COUNT_RECORDER(META_CLEANUP_JOB_STATE, "cache.meta.cleanup_job_state");
    CACHE_COUNT_RECORDER(MANAGER_ADMISSION_RESULT, "cache.manager.admission_result");
    CACHE_COUNT_RECORDER(MANAGER_LOADER_RESULT, "cache.manager.loader_result");
    CACHE_COUNT_RECORDER(MANAGER_CLEANUP_RESULT, "cache.manager.cleanup_result");
    CACHE_COUNT_RECORDER(MANAGER_SPACE_QUERY, "cache.manager.space_query");
    CACHE_COUNT_RECORDER(MANAGER_PREFLIGHT_RESULT, "cache.manager.preflight_result");
    CACHE_COUNT_RECORDER(MANAGER_EVICTION_RESULT, "cache.manager.eviction_result");
    CACHE_COUNT_RECORDER(MANAGER_LEASE_RECOVERY, "cache.manager.lease_recovery");
    CACHE_COUNT_RECORDER(MANAGER_ORCHESTRATION_TICK, "cache.manager.orchestration_tick");
    CACHE_COUNT_RECORDER(MANAGER_JOB_QUOTA_WAIT, "cache.manager.job_quota_wait");
    CACHE_COUNT_RECORDER(MANAGER_JOB_CANCEL, "cache.manager.job_cancel");
    CACHE_COUNT_RECORDER(STORAGE_PERMIT_RESULT, "cache.storage.permit_result");
    CACHE_COUNT_RECORDER(STORAGE_EVENT_PREPARED, "cache.storage.event_prepared");
    CACHE_COUNT_RECORDER(STORAGE_EVENT_DELIVERABLE, "cache.storage.event_deliverable");
    CACHE_COUNT_RECORDER(STORAGE_EVENT_ACKNOWLEDGED, "cache.storage.event_acknowledged");
    CACHE_COUNT_RECORDER(STORAGE_EVENT_JOURNAL_FAILURE, "cache.storage.event_journal_failure");
    CACHE_COUNT_RECORDER(STORAGE_GENERATION_REPLACE, "cache.storage.generation_replace");
    CACHE_COUNT_RECORDER(STORAGE_GENERATION_STALE, "cache.storage.generation_stale");
    CACHE_COUNT_RECORDER(STORAGE_TOMBSTONE, "cache.storage.tombstone");
    default:
      static monitor::CountRecorder invalid{"cache.invalid_count_event"};
      return invalid;
  }
#undef CACHE_COUNT_RECORDER
}

monitor::LatencyRecorder &latencyRecorder(Event event) {
#define CACHE_LATENCY_RECORDER(EVENT, NAME)         \
  case Event::EVENT: {                              \
    static monitor::LatencyRecorder recorder{NAME}; \
    return recorder;                                \
  }
  switch (event) {
    CACHE_LATENCY_RECORDER(CLIENT_READ_PLAN, "cache.client.read_plan_latency");
    CACHE_LATENCY_RECORDER(CLIENT_STORAGE_READ, "cache.client.storage_latency");
    CACHE_LATENCY_RECORDER(CLIENT_ORIGIN_READ, "cache.client.origin_latency");
    default:
      static monitor::LatencyRecorder invalid{"cache.invalid_latency_event"};
      return invalid;
  }
#undef CACHE_LATENCY_RECORDER
}

monitor::ValueRecorder &gaugeRecorder(Event event) {
#define CACHE_GAUGE_RECORDER(EVENT, NAME)                              \
  case Event::EVENT: {                                                 \
    static monitor::ValueRecorder recorder{NAME, std::nullopt, false}; \
    return recorder;                                                   \
  }
  switch (event) {
    CACHE_GAUGE_RECORDER(MANAGER_QUEUE, "cache.manager.queue");
    CACHE_GAUGE_RECORDER(MANAGER_INFLIGHT_BYTES, "cache.manager.inflight_bytes");
    CACHE_GAUGE_RECORDER(MANAGER_PHYSICAL_USED_BYTES, "cache.manager.physical_used_bytes");
    CACHE_GAUGE_RECORDER(MANAGER_ALLOCATABLE_BYTES, "cache.manager.allocatable_bytes");
    CACHE_GAUGE_RECORDER(MANAGER_RESERVED_BYTES, "cache.manager.reserved_bytes");
    CACHE_GAUGE_RECORDER(MANAGER_SNAPSHOT_AGE_NS, "cache.manager.snapshot_age_ns");
    CACHE_GAUGE_RECORDER(MANAGER_ACTIVE_JOBS, "cache.manager.active_jobs");
    CACHE_GAUGE_RECORDER(MANAGER_JOB_INFLIGHT, "cache.manager.job_inflight");
    CACHE_GAUGE_RECORDER(MANAGER_JOB_READY_BPS, "cache.manager.job_ready_bps");
    CACHE_GAUGE_RECORDER(MANAGER_PINNED_BYTES, "cache.manager.pinned_bytes");
    CACHE_GAUGE_RECORDER(STORAGE_EVENT_BACKLOG, "cache.storage.event_backlog");
    default:
      static monitor::ValueRecorder invalid{"cache.invalid_gauge_event", std::nullopt, false};
      return invalid;
  }
#undef CACHE_GAUGE_RECORDER
}

}  // namespace

void recordCount(Event event, uint64_t value, const Tags &tags) {
  if (!valid(event)) return;
  counters[index(event)].fetch_add(value, std::memory_order_relaxed);
  remember(event, tags);
  countRecorder(event).addSample(static_cast<int64_t>(std::min<uint64_t>(value, std::numeric_limits<int64_t>::max())),
                                 toTagSet(tags));
}

void recordLatency(Event event, std::chrono::nanoseconds latency, const Tags &tags) {
  if (!valid(event)) return;
  counters[index(event)].fetch_add(1, std::memory_order_relaxed);
  remember(event, tags);
  latencyRecorder(event).addSample(latency, toTagSet(tags));
}

void setGauge(Event event, int64_t value, const Tags &tags) {
  if (!valid(event)) return;
  counters[index(event)].store(value < 0 ? 0 : static_cast<uint64_t>(value), std::memory_order_relaxed);
  remember(event, tags);
  gaugeRecorder(event).set(value, toTagSet(tags));
}

uint64_t countForTest(Event event) { return valid(event) ? counters[index(event)].load(std::memory_order_relaxed) : 0; }

Tags lastTagsForTest(Event event) {
  if (!valid(event)) return {};
  auto lock = std::unique_lock(tagsMutex);
  return lastTags[index(event)];
}

void resetForTest() {
  for (auto &counter : counters) counter.store(0, std::memory_order_relaxed);
  auto lock = std::unique_lock(tagsMutex);
  lastTags.fill({});
}

}  // namespace hf3fs::cache::metrics
