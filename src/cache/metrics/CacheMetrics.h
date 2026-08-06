#pragma once

#include <array>
#include <chrono>
#include <cstdint>
#include <optional>
#include <string>

namespace hf3fs::cache::metrics {

enum class Event : uint8_t {
  CLIENT_HIT_BYTES,
  CLIENT_MISS_BYTES,
  CLIENT_ORIGIN_BYTES,
  CLIENT_READ_PLAN,
  CLIENT_STORAGE_READ,
  CLIENT_ORIGIN_READ,
  CLIENT_HINT_RESULT,
  CLIENT_REPORT_RESULT,
  META_STATE_TRANSITION,
  META_CHARGE_BYTES,
  META_EPOCH_CONFLICT,
  META_CLEANUP_JOB_STATE,
  MANAGER_QUEUE,
  MANAGER_INFLIGHT_BYTES,
  MANAGER_ADMISSION_RESULT,
  MANAGER_LOADER_RESULT,
  MANAGER_CLEANUP_RESULT,
  MANAGER_SPACE_QUERY,
  MANAGER_PHYSICAL_USED_BYTES,
  MANAGER_ALLOCATABLE_BYTES,
  MANAGER_RESERVED_BYTES,
  MANAGER_SNAPSHOT_AGE_NS,
  MANAGER_PREFLIGHT_RESULT,
  MANAGER_EVICTION_RESULT,
  MANAGER_ORCHESTRATION_TICK,
  MANAGER_ACTIVE_JOBS,
  MANAGER_JOB_INFLIGHT,
  MANAGER_JOB_QUOTA_WAIT,
  MANAGER_JOB_READY_BPS,
  MANAGER_JOB_CANCEL,
  MANAGER_PINNED_BYTES,
  STORAGE_PERMIT_RESULT,
  STORAGE_EVENT_PREPARED,
  STORAGE_EVENT_DELIVERABLE,
  STORAGE_EVENT_ACKNOWLEDGED,
  STORAGE_EVENT_BACKLOG,
  STORAGE_EVENT_JOURNAL_FAILURE,
  STORAGE_GENERATION_REPLACE,
  STORAGE_GENERATION_STALE,
  STORAGE_TOMBSTONE,
  COUNT,
};

struct Tags {
  std::optional<uint64_t> inode;
  std::optional<uint32_t> block;
  std::optional<uint32_t> originId;
  std::string diskId;
  std::string generation;
  std::string epoch;
  std::string operationId;
  std::string sourceId;
  std::optional<uint64_t> sequence;
  std::string reason;
};

void recordCount(Event event, uint64_t value = 1, const Tags &tags = {});
void recordLatency(Event event, std::chrono::nanoseconds latency, const Tags &tags = {});
void setGauge(Event event, int64_t value, const Tags &tags = {});

uint64_t countForTest(Event event);
Tags lastTagsForTest(Event event);
void resetForTest();

}  // namespace hf3fs::cache::metrics
