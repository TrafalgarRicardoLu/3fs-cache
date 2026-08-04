#include "meta/store/cache/OrchestrationKey.h"

#include <folly/lang/Bits.h>

#include "common/kv/KeyPrefix.h"
#include "common/utils/SerDeser.h"

namespace hf3fs::meta::server {
namespace {

uint64_t ordered(uint64_t value) { return folly::Endian::big(value); }
uint32_t ordered(uint32_t value) { return folly::Endian::big(value); }

Result<Void> validJobId(cache::PrefetchJobId jobId) {
  if (jobId == cache::PrefetchJobId{}) return makeError(StatusCode::kInvalidArg, "prefetch job id not set");
  return Void{};
}

Result<Void> validOwner(const cache::PinOwner &owner) { return owner.valid(); }

}  // namespace

std::string OrchestrationKey::jobPrefix() { return Serializer::serRawArgs(kv::KeyPrefix::PrefetchJob); }

std::string OrchestrationKey::job(cache::PrefetchJobId jobId) {
  return Serializer::serRawArgs(kv::KeyPrefix::PrefetchJob, jobId.toUnderType());
}

Result<cache::PrefetchJobId> OrchestrationKey::unpackJob(std::string_view key) {
  kv::KeyPrefix prefix;
  Uuid jobId;
  RETURN_ON_ERROR(Deserializer::deserRawArgs(key, prefix, jobId));
  if (prefix != kv::KeyPrefix::PrefetchJob) return makeError(StatusCode::kDataCorruption, "invalid job key prefix");
  auto result = cache::PrefetchJobId{jobId};
  RETURN_ON_ERROR(validJobId(result));
  return result;
}

std::string OrchestrationKey::planPrefix(cache::PrefetchJobId jobId) {
  return Serializer::serRawArgs(kv::KeyPrefix::PrefetchPlan, jobId.toUnderType());
}

std::string OrchestrationKey::plan(cache::PrefetchJobId jobId, const cache::CacheBlockKey &block) {
  return Serializer::serRawArgs(kv::KeyPrefix::PrefetchPlan,
                                jobId.toUnderType(),
                                ordered(block.inode),
                                ordered(block.block.toUnderType()));
}

Result<PrefetchPlanKey> OrchestrationKey::unpackPlan(std::string_view key) {
  kv::KeyPrefix prefix;
  Uuid jobId;
  uint64_t inode;
  uint32_t block;
  RETURN_ON_ERROR(Deserializer::deserRawArgs(key, prefix, jobId, inode, block));
  PrefetchPlanKey result{cache::PrefetchJobId{jobId}, {ordered(inode), cache::CacheBlockIndex{ordered(block)}}};
  if (prefix != kv::KeyPrefix::PrefetchPlan || validJobId(result.jobId).hasError() || result.block.valid().hasError()) {
    return makeError(StatusCode::kDataCorruption, "invalid prefetch plan key");
  }
  return result;
}

std::string OrchestrationKey::pinByBlockPrefix(const cache::CacheBlockKey &block) {
  return Serializer::serRawArgs(kv::KeyPrefix::PinByBlock, ordered(block.inode), ordered(block.block.toUnderType()));
}

std::string OrchestrationKey::pinByBlock(const cache::CacheBlockKey &block, const cache::PinOwner &owner) {
  return Serializer::serRawArgs(kv::KeyPrefix::PinByBlock,
                                ordered(block.inode),
                                ordered(block.block.toUnderType()),
                                static_cast<uint8_t>(owner.kind),
                                owner.id.toUnderType());
}

Result<PinIndexKey> OrchestrationKey::unpackPinByBlock(std::string_view key) {
  kv::KeyPrefix prefix;
  uint64_t inode;
  uint32_t block;
  uint8_t ownerKind;
  Uuid ownerId;
  RETURN_ON_ERROR(Deserializer::deserRawArgs(key, prefix, inode, block, ownerKind, ownerId));
  PinIndexKey result{{ordered(inode), cache::CacheBlockIndex{ordered(block)}},
                     {static_cast<cache::PinOwnerKind>(ownerKind), cache::PinOwnerId{ownerId}}};
  if (prefix != kv::KeyPrefix::PinByBlock || result.block.valid().hasError() || validOwner(result.owner).hasError()) {
    return makeError(StatusCode::kDataCorruption, "invalid pin-by-block key");
  }
  return result;
}

std::string OrchestrationKey::pinByOwnerPrefix(const cache::PinOwner &owner) {
  return Serializer::serRawArgs(kv::KeyPrefix::PinByOwner, static_cast<uint8_t>(owner.kind), owner.id.toUnderType());
}

std::string OrchestrationKey::pinByOwner(const cache::PinOwner &owner, const cache::CacheBlockKey &block) {
  return Serializer::serRawArgs(kv::KeyPrefix::PinByOwner,
                                static_cast<uint8_t>(owner.kind),
                                owner.id.toUnderType(),
                                ordered(block.inode),
                                ordered(block.block.toUnderType()));
}

Result<PinIndexKey> OrchestrationKey::unpackPinByOwner(std::string_view key) {
  kv::KeyPrefix prefix;
  uint8_t ownerKind;
  Uuid ownerId;
  uint64_t inode;
  uint32_t block;
  RETURN_ON_ERROR(Deserializer::deserRawArgs(key, prefix, ownerKind, ownerId, inode, block));
  PinIndexKey result{{ordered(inode), cache::CacheBlockIndex{ordered(block)}},
                     {static_cast<cache::PinOwnerKind>(ownerKind), cache::PinOwnerId{ownerId}}};
  if (prefix != kv::KeyPrefix::PinByOwner || result.block.valid().hasError() || validOwner(result.owner).hasError()) {
    return makeError(StatusCode::kDataCorruption, "invalid pin-by-owner key");
  }
  return result;
}

}  // namespace hf3fs::meta::server
