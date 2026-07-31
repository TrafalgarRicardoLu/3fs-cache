#pragma once

#include <algorithm>
#include <optional>
#include <span>
#include <vector>

#include "common/utils/Coroutine.h"
#include "fbs/storage/Cache.h"

namespace hf3fs::storage {

class CachePermitCoordinator {
 public:
  template <typename Node, typename Query, typename Prepare, typename Release>
  static CoTryTask<CachePermitResult> prepare(std::span<const Node> nodes,
                                              Query query,
                                              Prepare prepare,
                                              Release release) {
    std::vector<const Node *> rollbackNodes;
    Result<CachePermitResult> result = makeError(CacheCode::kUnavailable);
    for (const auto &node : nodes) {
      auto current = co_await query(node);
      if (!current) {
        auto code = current.error().code();
        if (code != CacheCode::kNotFound && code != CacheCode::kPermitExpired)
          co_return makeError(std::move(current.error()));
        rollbackNodes.push_back(&node);
      }
      result = co_await prepare(node);
      if (!result) break;
    }
    if (!result) {
      for (auto *node : rollbackNodes) {
        (void)co_await release(*node);
      }
    }
    co_return result;
  }

  template <typename Node, typename Renew>
  static CoTryTask<CachePermitResult> renew(std::span<const Node> nodes, Renew renew) {
    Result<CachePermitResult> result = makeError(CacheCode::kUnavailable);
    std::optional<Status> firstError;
    for (const auto &node : nodes) {
      auto current = co_await renew(node);
      if (!current) {
        if (!firstError) firstError = current.error();
      } else {
        result = std::move(current);
      }
    }
    if (firstError) result = makeError(std::move(*firstError));
    co_return result;
  }

  template <typename Node, typename Release>
  static CoTryTask<Void> release(std::span<const Node> nodes, Release release) {
    Result<Void> result = Void{};
    for (const auto &node : nodes) {
      auto current = co_await release(node);
      if (!current && result) result = makeError(std::move(current.error()));
    }
    co_return result;
  }

  template <typename Node, typename Query>
  static CoTryTask<CachePermitResult> query(std::span<const Node> nodes, Query query) {
    std::optional<CachePermitResult> active;
    std::optional<Status> queryError;
    for (const auto &node : nodes) {
      auto current = co_await query(node);
      if (!current) {
        auto code = current.error().code();
        if (code != CacheCode::kNotFound && code != CacheCode::kPermitExpired && !queryError)
          queryError = current.error();
        continue;
      }
      if (!active) {
        active = *current;
      } else {
        if (current->permit != active->permit) {
          auto mismatch = makeError(CacheCode::kInvalidResponse, "cache replica permit identity mismatch");
          queryError = mismatch.error();
        }
        if (current->state == cache::CachePermitState::PINNED) active->state = current->state;
        active->expiresAtNs = std::max(active->expiresAtNs, current->expiresAtNs);
      }
    }
    if (queryError) co_return makeError(std::move(*queryError));
    if (active) co_return std::move(*active);
    co_return makeError(CacheCode::kNotFound, "permit is not active on any replica");
  }
};

}  // namespace hf3fs::storage
