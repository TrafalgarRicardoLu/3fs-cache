#include "meta/store/cache/CacheBlockRecord.h"

namespace hf3fs::meta::server {

Result<Void> CacheBlockRecord::valid() const {
  RETURN_ON_ERROR(key.valid());
  switch (state) {
    case cache::CacheBlockState::QUEUED:
    case cache::CacheBlockState::LOADING:
    case cache::CacheBlockState::READY:
    case cache::CacheBlockState::CLEANING:
    case cache::CacheBlockState::FAILED:
    case cache::CacheBlockState::INVALID:
    case cache::CacheBlockState::EVICTING:
      break;
    case cache::CacheBlockState::NONE:
      return makeError(StatusCode::kInvalidArg, "NONE is not persisted");
    default:
      return makeError(StatusCode::kInvalidArg, "invalid cache block state");
  }

  switch (chargeKind) {
    case cache::ChargeKind::NONE:
      if (chargedBytes != 0) return makeError(StatusCode::kInvalidArg, "uncharged record has charged bytes");
      break;
    case cache::ChargeKind::RESERVED:
    case cache::ChargeKind::COMMITTED:
      if (chargedBytes == 0 || chargedBytes != blockLength) {
        return makeError(StatusCode::kInvalidArg, "charged bytes must equal block length");
      }
      break;
    default:
      return makeError(StatusCode::kInvalidArg, "invalid cache charge kind");
  }

  if (state == cache::CacheBlockState::FAILED && chargeKind != cache::ChargeKind::NONE) {
    return makeError(StatusCode::kInvalidArg, "FAILED record cannot retain a charge");
  }
  if (state == cache::CacheBlockState::READY && chargeKind != cache::ChargeKind::COMMITTED) {
    return makeError(StatusCode::kInvalidArg, "READY record requires a committed charge");
  }
  if (state == cache::CacheBlockState::LOADING &&
      (loaderId == Uuid::zero() || loadEpoch == 0 || cacheGeneration == cache::CacheGeneration{} ||
       leaseExpiresAt.isZero())) {
    return makeError(StatusCode::kInvalidArg, "LOADING record requires a complete lease fence");
  }
  if (ready.has_value() && state != cache::CacheBlockState::READY && state != cache::CacheBlockState::CLEANING &&
      state != cache::CacheBlockState::EVICTING) {
    return makeError(StatusCode::kInvalidArg, "ready identity is not valid for this state");
  }
  if (ready.has_value()) RETURN_ON_ERROR(ready->valid());
  if (permit.has_value()) RETURN_ON_ERROR(permit->valid());
  if (placement.has_value()) RETURN_ON_ERROR(placement->valid());
  if (committedPermit.has_value()) RETURN_ON_ERROR(committedPermit->valid());
  if (permit.has_value() && placement.has_value()) {
    return makeError(StatusCode::kInvalidArg, "cache record cannot retain permit and placement together");
  }
  if (permit.has_value() && committedPermit.has_value()) {
    return makeError(StatusCode::kInvalidArg, "cache record cannot retain active and committed permits together");
  }
  if (committedPermit.has_value() && (!placement.has_value() || committedPermit->placement != *placement)) {
    return makeError(StatusCode::kInvalidArg, "committed permit requires its immutable placement");
  }
  const bool hasPhase2Identity = permit.has_value() || placement.has_value() || committedPermit.has_value();
  if (hasPhase2Identity) {
    switch (state) {
      case cache::CacheBlockState::QUEUED:
      case cache::CacheBlockState::LOADING:
        if (!permit.has_value() || committedPermit.has_value())
          return makeError(StatusCode::kInvalidArg, "admitted record requires active permit");
        break;
      case cache::CacheBlockState::READY:
      case cache::CacheBlockState::EVICTING:
        if (!placement.has_value() || !committedPermit.has_value())
          return makeError(StatusCode::kInvalidArg, "materialized record requires committed placement");
        break;
      case cache::CacheBlockState::CLEANING:
        if (!placement.has_value()) return makeError(StatusCode::kInvalidArg, "cleanup record requires placement");
        break;
      case cache::CacheBlockState::FAILED:
      case cache::CacheBlockState::INVALID:
      case cache::CacheBlockState::NONE:
        return makeError(StatusCode::kInvalidArg, "terminal record cannot retain phase two identity");
    }
  }
  return Void{};
}

}  // namespace hf3fs::meta::server
