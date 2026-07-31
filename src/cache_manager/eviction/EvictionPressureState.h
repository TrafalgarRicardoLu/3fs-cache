#pragma once

#include <mutex>
#include <set>

#include "fbs/storage/Common.h"

namespace hf3fs::cache_manager {

class EvictionPressureState {
 public:
  void replace(std::set<storage::PhysicalDiskId> pressured) {
    std::scoped_lock lock(mutex_);
    pressured_ = std::move(pressured);
  }

  bool contains(storage::PhysicalDiskId diskId) const {
    std::scoped_lock lock(mutex_);
    return pressured_.contains(diskId);
  }

  std::set<storage::PhysicalDiskId> snapshot() const {
    std::scoped_lock lock(mutex_);
    return pressured_;
  }

 private:
  mutable std::mutex mutex_;
  std::set<storage::PhysicalDiskId> pressured_;
};

}  // namespace hf3fs::cache_manager
