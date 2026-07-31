#pragma once

#include <map>
#include <memory>
#include <mutex>

#include "cache_manager/capacity/PhysicalTopology.h"

namespace hf3fs::cache_manager {

class PhysicalPreflight {
 private:
  struct SharedState;

 public:
  class Reservation {
   public:
    Reservation() = default;
    Reservation(const Reservation &) = delete;
    Reservation &operator=(const Reservation &) = delete;
    Reservation(Reservation &&other) noexcept;
    Reservation &operator=(Reservation &&other) noexcept;
    ~Reservation();

    const auto &bytesByDisk() const { return bytesByDisk_; }

   private:
    Reservation(std::shared_ptr<SharedState> state, std::map<storage::PhysicalDiskId, uint64_t> bytesByDisk);
    void release();

    std::shared_ptr<SharedState> state_;
    std::map<storage::PhysicalDiskId, uint64_t> bytesByDisk_;
    friend class PhysicalPreflight;
  };

  PhysicalPreflight(const PhysicalTopology &topology, Duration maxAge, double highWatermark);

  Result<Reservation> tryReserve(flat::ChainId chainId, const storage::FootprintByTarget &footprints, SteadyTime now);
  uint64_t locallyReserved(storage::PhysicalDiskId diskId) const;

 private:
  struct SharedState {
    mutable std::mutex mutex;
    std::map<storage::PhysicalDiskId, uint64_t> reserved;
  };

  const PhysicalTopology &topology_;
  const Duration maxAge_;
  const double highWatermark_;
  std::shared_ptr<SharedState> state_;
};

}  // namespace hf3fs::cache_manager
