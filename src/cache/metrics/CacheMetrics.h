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
  STORAGE_GENERATION_REPLACE,
  STORAGE_GENERATION_STALE,
  STORAGE_TOMBSTONE,
  COUNT,
};

struct Tags {
  std::optional<uint64_t> inode;
  std::optional<uint32_t> block;
  std::optional<uint32_t> originId;
  std::string reason;
};

void recordCount(Event event, uint64_t value = 1, const Tags &tags = {});
void recordLatency(Event event, std::chrono::nanoseconds latency, const Tags &tags = {});
void setGauge(Event event, int64_t value, const Tags &tags = {});

uint64_t countForTest(Event event);
Tags lastTagsForTest(Event event);
void resetForTest();

}  // namespace hf3fs::cache::metrics
