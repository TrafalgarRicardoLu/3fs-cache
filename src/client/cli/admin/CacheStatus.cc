#include "CacheStatus.h"

#include "AdminEnv.h"
#include "client/cli/common/Dispatcher.h"
#include "client/cli/common/Utils.h"
#include "common/utils/MagicEnum.hpp"

namespace hf3fs::client::cli {
namespace {

auto getParser() {
  argparse::ArgumentParser parser("cache-status");
  parser.add_argument("--inode").scan<'u', uint64_t>();
  return parser;
}

CoTryTask<Dispatcher::OutputTable> handle(IEnv &ienv,
                                          const argparse::ArgumentParser &parser,
                                          const Dispatcher::Args &args) {
  auto &env = dynamic_cast<AdminEnv &>(ienv);
  ENSURE_USAGE(args.empty());
  meta::GetCacheStatusReq request;
  request.user = env.userInfo;
  if (auto inode = parser.present<uint64_t>("--inode")) request.inode = meta::InodeId{*inode};
  request.cacheProtocolVersion = cache::kCacheProtocolVersion;
  auto result = co_await env.metaClientGetter()->getCacheStatus(std::move(request));
  CO_RETURN_ON_ERROR(result);

  cache_manager::GetCacheStatusReq managerRequest;
  managerRequest.user = env.userInfo;
  if (auto inode = parser.present<uint64_t>("--inode")) managerRequest.inode = meta::InodeId{*inode};
  managerRequest.cacheProtocolVersion = cache::kCacheProtocolVersion;
  auto managerResult = co_await env.cacheManagerStubGetter()->getCacheStatus(managerRequest);
  CO_RETURN_ON_ERROR(managerResult);

  cache_manager::GetPhase2CacheStatusReq phase2Request;
  phase2Request.user = env.userInfo;
  phase2Request.cacheProtocolVersion = cache::kCacheProtocolVersion;
  auto phase2Result = co_await env.cacheManagerStubGetter()->getPhase2CacheStatus(phase2Request);

  Dispatcher::OutputTable table{
      {"Metric", "Value"},
      {"logical_reference_capacity", std::to_string(result->logicalCapacity)},
      {"logical_used_bytes", std::to_string(result->usedCapacity)},
      {"logical_reserved_bytes", std::to_string(result->reservedCapacity)},
      {"logical_committed_bytes", std::to_string(result->committedCapacity)},
      {"logical_capacity_semantics", "informational_only"},
      {"admission_capacity_source", "storage_physical_space_gate"},
      {"manager.queued", std::to_string(managerResult->queued)},
      {"manager.loading", std::to_string(managerResult->loading)},
      {"manager.inflight_bytes", std::to_string(managerResult->inflightBytes)},
      {"manager.ready", std::to_string(managerResult->ready)},
      {"manager.cleaning", std::to_string(managerResult->cleaning)},
      {"manager.last_bypass_reason", std::string(magic_enum::enum_name(managerResult->lastBypassReason))}};
  table.push_back({"phase3.enabled", managerResult->phase3Enabled ? "true" : "false"});
  table.push_back({"phase3.active_jobs", std::to_string(managerResult->activeJobs)});
  table.push_back({"phase3.failed_jobs", std::to_string(managerResult->failedJobs)});
  table.push_back({"phase3.planned_bytes", std::to_string(managerResult->phase3PlannedBytes)});
  table.push_back({"phase3.ready_bytes", std::to_string(managerResult->phase3ReadyBytes)});
  table.push_back({"phase3.pinned_bytes", std::to_string(managerResult->pinnedBytes)});
  table.push_back({"phase3.last_job_error", managerResult->lastJobError});
  if (phase2Result.hasError()) {
    table.push_back({"phase2.available", "false"});
    table.push_back({"phase2.error", phase2Result.error().describe()});
  } else {
    table.push_back({"phase2.available", "true"});
    table.push_back({"phase2.enabled", phase2Result->enabled ? "true" : "false"});
    table.push_back({"phase2.manager_epoch", phase2Result->managerEpoch.toHexString()});
    table.push_back({"phase2.admission_policy", phase2Result->admissionPolicy});
    table.push_back({"phase2.eviction_policy", phase2Result->evictionPolicy});
    table.push_back({"phase2.capacity_high_watermark", std::to_string(phase2Result->capacityHighWatermark)});
    table.push_back({"phase2.capacity_low_watermark", std::to_string(phase2Result->capacityLowWatermark)});
    table.push_back({"phase2.snapshot_max_age_ns", std::to_string(phase2Result->snapshotMaxAgeNs)});
    table.push_back({"phase2.permit_ttl_ns", std::to_string(phase2Result->permitTtlNs)});
    table.push_back({"phase2.evicting", std::to_string(phase2Result->evicting)});
    table.push_back({"phase2.event_backlog", std::to_string(phase2Result->eventBacklog)});
    table.push_back({"phase2.dead_letters", std::to_string(phase2Result->deadLetters)});
    for (const auto &disk : phase2Result->disks) {
      auto prefix = fmt::format("phase2.disk.{}", disk.physicalDiskId.uuid.toHexString());
      table.push_back({prefix + ".role", std::string(magic_enum::enum_name(disk.role))});
      table.push_back({prefix + ".capacity_bytes", std::to_string(disk.capacityBytes)});
      table.push_back({prefix + ".physical_used_bytes", std::to_string(disk.physicalUsedBytes)});
      table.push_back({prefix + ".allocatable_bytes", std::to_string(disk.allocatableBytes)});
      table.push_back({prefix + ".reserved_bytes", std::to_string(disk.reservedBytes)});
      table.push_back({prefix + ".active_generations", std::to_string(disk.activeGenerations)});
      table.push_back({prefix + ".snapshot_age_ns", std::to_string(disk.snapshotAgeNs)});
      table.push_back({prefix + ".admission_paused", disk.admissionPaused ? "true" : "false"});
      table.push_back({prefix + ".pause_reason", disk.pauseReason});
      table.push_back({prefix + ".event_prepared", std::to_string(disk.eventPrepared)});
      table.push_back({prefix + ".event_deliverable", std::to_string(disk.eventDeliverable)});
      table.push_back({prefix + ".event_ack_sequence", std::to_string(disk.eventAcknowledgedSequence)});
      table.push_back({prefix + ".enforced_high_watermark", std::to_string(disk.enforcedHighWatermark)});
      table.push_back({prefix + ".permit_store_healthy", disk.permitStoreHealthy ? "true" : "false"});
      table.push_back({prefix + ".event_journal_writable", disk.eventJournalWritable ? "true" : "false"});
    }
  }
  for (const auto &count : result->stateCounts) {
    table.push_back({fmt::format("state.{}", magic_enum::enum_name(count.state)), std::to_string(count.count)});
  }
  for (const auto &count : result->chargeCounts) {
    table.push_back({fmt::format("charge.{}.count", magic_enum::enum_name(count.kind)), std::to_string(count.count)});
    table.push_back({fmt::format("charge.{}.bytes", magic_enum::enum_name(count.kind)), std::to_string(count.bytes)});
  }
  co_return table;
}

}  // namespace

CoTryTask<void> registerCacheStatusHandler(Dispatcher &dispatcher) {
  co_return co_await dispatcher.registerHandler(getParser, handle);
}

}  // namespace hf3fs::client::cli
