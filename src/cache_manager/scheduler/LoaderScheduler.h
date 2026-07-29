#pragma once

#include "cache_manager/loader/CacheLoader.h"
#include "cache_manager/scheduler/HintCoalescer.h"

namespace hf3fs::cache_manager {

class LoaderScheduler {
 public:
  LoaderScheduler(HintCoalescer &hints, CacheLoader &loader, uint64_t maxRangeBytes)
      : hints_(hints),
        loader_(loader),
        maxRangeBytes_(maxRangeBytes) {}

  CoTask<void> runOne();

 private:
  HintCoalescer &hints_;
  CacheLoader &loader_;
  uint64_t maxRangeBytes_;
};

}  // namespace hf3fs::cache_manager
