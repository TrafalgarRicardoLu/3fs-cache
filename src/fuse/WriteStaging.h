#pragma once

#include <cstdint>
#include <string>
#include <string_view>

#include "common/utils/Path.h"
#include "common/utils/Result.h"
#include "fbs/meta/Schema.h"

namespace hf3fs::fuse {

inline bool isWriteStagingInode(const meta::Inode &inode, flat::ChainTableId tableId) {
  return tableId && inode.isFile() && inode.asFile().layout.tableId == tableId;
}

inline Result<Void> checkSequentialWrite(uint64_t nextOffset, int64_t offset, size_t size) {
  if (offset < 0 || static_cast<uint64_t>(offset) != nextOffset) {
    return makeError(StatusCode::kInvalidArg, "write-through staging only accepts sequential writes");
  }
  if (size > UINT64_MAX - nextOffset) {
    return makeError(StatusCode::kInvalidArg, "write-through staging offset overflow");
  }
  return Void{};
}

inline std::string writeStagingObjectKey(std::string_view prefix, const Path &namespacePath, const Uuid &jobId) {
  auto normalized = namespacePath.lexically_normal().generic_string();
  while (!normalized.empty() && normalized.front() == '/') normalized.erase(normalized.begin());

  std::string key(prefix);
  while (!key.empty() && key.back() == '/') key.pop_back();
  if (!key.empty() && !normalized.empty()) key.push_back('/');
  key.append(normalized);
  if (!key.empty()) key.push_back('/');
  key.append(jobId.toHexString());
  return key;
}

}  // namespace hf3fs::fuse
