#pragma once

#include "ChainInfo.h"
#include "common/serde/SerdeComparisons.h"

namespace hf3fs::flat {
struct ChainTable : public serde::SerdeHelper<ChainTable> {
  bool operator==(const ChainTable &other) const { return serde::equals(*this, other); }

  SERDE_STRUCT_FIELD(chainTableId, ChainTableId(0));
  SERDE_STRUCT_FIELD(chainTableVersion, ChainTableVersion(0));
  SERDE_STRUCT_FIELD(chains, std::vector<ChainId>{});
  SERDE_STRUCT_FIELD(desc, String{});
  SERDE_STRUCT_FIELD(role, ChainTableRole::USER_DATA);
  SERDE_STRUCT_FIELD(logicalCapacity, uint64_t{0});
  SERDE_STRUCT_FIELD(checksumType, ChainTableChecksumType::NONE);

 public:
  bool isCacheData() const { return role == ChainTableRole::CACHE_DATA; }
  bool isWriteStaging() const { return role == ChainTableRole::WRITE_STAGING; }
  Result<Void> valid() const {
    switch (role) {
      case ChainTableRole::USER_DATA:
      case ChainTableRole::WRITE_STAGING:
        if (logicalCapacity != 0 || checksumType != ChainTableChecksumType::NONE) {
          return makeError(StatusCode::kInvalidArg, "non-cache table cannot set cache capacity or checksum");
        }
        break;
      case ChainTableRole::CACHE_DATA:
        if (logicalCapacity == 0) {
          return makeError(StatusCode::kInvalidArg, "CACHE_DATA table requires a non-zero logical capacity");
        }
        switch (checksumType) {
          case ChainTableChecksumType::CRC32C:
          case ChainTableChecksumType::CRC32:
            break;
          case ChainTableChecksumType::NONE:
            return makeError(StatusCode::kInvalidArg, "CACHE_DATA table requires a checksum");
          default:
            return makeError(StatusCode::kInvalidArg, "unknown CACHE_DATA checksum type");
        }
        break;
      default:
        return makeError(StatusCode::kInvalidArg, "unknown chain table role");
    }
    return Void{};
  }
};
}  // namespace hf3fs::flat
