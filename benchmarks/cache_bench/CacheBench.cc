#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstring>
#include <folly/experimental/coro/BlockingWait.h>
#include <folly/init/Init.h>
#include <fstream>
#include <gflags/gflags.h>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "cache/origin/ObjectStore.h"
#include "client/cache/CacheReadPipeline.h"

DEFINE_uint64(iterations, 100, "Iterations per cache benchmark scenario");
DEFINE_uint64(object_size, 4ULL << 20, "Synthetic object size in bytes");
DEFINE_uint64(read_size, 256ULL << 10, "Read size in bytes; must be block aligned");
DEFINE_uint32(block_size, 64U << 10, "Cache block size in bytes");
DEFINE_string(output, "", "Optional CSV output path");

namespace hf3fs::cache::benchmark {
namespace {

enum class PlanMode { MISS, WARM, MIXED };

class MemoryObjectStore final : public origin::ObjectStore {
 public:
  explicit MemoryObjectStore(std::vector<uint8_t> data)
      : data_(std::move(data)) {}

  CoTryTask<origin::ObjectMetadata> head(const ObjectRef &) final { co_return makeError(StatusCode::kNotImplemented); }

  CoTryTask<std::vector<uint8_t>> getRange(const ImmutableObjectIdentity &, ByteRange range) final {
    ++requests;
    auto end = range.end();
    CO_RETURN_ON_ERROR(end);
    if (*end > data_.size()) co_return makeError(CacheCode::kInvalidResponse);
    co_return std::vector<uint8_t>(data_.begin() + range.offset, data_.begin() + *end);
  }

  std::atomic<uint64_t> requests{0};

 private:
  std::vector<uint8_t> data_;
};

class BenchmarkPlanSource final : public client::cache::IReadPlanSource {
 public:
  BenchmarkPlanSource(meta::Inode inode, const std::vector<uint8_t> &data, uint32_t blockSize, PlanMode mode)
      : inode_(std::move(inode)),
        data_(data),
        blockSize_(blockSize),
        mode_(mode) {}

  CoTryTask<meta::GetFileReadPlanRsp> fetch(meta::GetFileReadPlanReq request) final {
    meta::GetFileReadPlanRsp response;
    response.inode = inode_.id;
    response.object = inode_.asOriginFile().object;
    auto requestEnd = std::min(inode_.fileLength(), request.offset + request.length);
    for (auto index = request.offset / blockSize_; index * blockSize_ < requestEnd; ++index) {
      auto begin = index * blockSize_;
      auto length = std::min<uint64_t>(blockSize_, inode_.fileLength() - begin);
      meta::ReadBlockPlan block;
      block.key = {inode_.id.u64(), CacheBlockIndex{static_cast<uint32_t>(index)}};
      block.fileRange = {begin, length};
      block.originRange = block.fileRange;
      block.chunkId = meta::ChunkId(inode_.id, 0, static_cast<uint32_t>(index));
      block.chainId = flat::ChainId{1};
      block.actualBlockLength = length;
      auto ready = mode_ == PlanMode::WARM || (mode_ == PlanMode::MIXED && index % 2 == 0);
      block.state = ready ? CacheBlockState::READY : CacheBlockState::LOADING;
      if (ready) {
        block.loadEpoch = 1;
        auto checksum = storage::ChecksumInfo::create(storage::ChecksumType::CRC32C, data_.data() + begin, length);
        block.ready = ReadyIdentity{block.loadEpoch,
                                    CacheGeneration{1},
                                    static_cast<uint8_t>(checksum.type),
                                    checksum.value,
                                    length};
      }
      response.blocks.push_back(std::move(block));
    }
    co_return response;
  }

 private:
  meta::Inode inode_;
  const std::vector<uint8_t> &data_;
  uint32_t blockSize_;
  PlanMode mode_;
};

class BenchmarkHitReader final : public client::cache::ICacheHitReader {
 public:
  explicit BenchmarkHitReader(const std::vector<uint8_t> &data)
      : data_(data) {}

  CoTryTask<std::vector<uint8_t>> readFullBlock(const meta::ReadBlockPlan &plan, const flat::UserInfo &) final {
    hitBytes += plan.fileRange.length;
    co_return std::vector<uint8_t>(data_.begin() + plan.fileRange.offset,
                                   data_.begin() + plan.fileRange.offset + plan.fileRange.length);
  }

  std::atomic<uint64_t> hitBytes{0};

 private:
  const std::vector<uint8_t> &data_;
};

struct ScenarioResult {
  std::string name;
  double throughputMiB{0};
  double p50Us{0};
  double p99Us{0};
  uint64_t originRequests{0};
  double hitRatio{0};
};

template <typename Read, typename OriginRequests, typename HitBytes>
Result<ScenarioResult> runScenario(std::string name,
                                   uint64_t iterations,
                                   uint64_t bytesPerRead,
                                   uint64_t originBefore,
                                   uint64_t hitBefore,
                                   Read &&read,
                                   OriginRequests &&originRequests,
                                   HitBytes &&hitBytes) {
  std::vector<double> latencies;
  latencies.reserve(iterations);
  auto scenarioStart = std::chrono::steady_clock::now();
  for (uint64_t iteration = 0; iteration < iterations; ++iteration) {
    auto start = std::chrono::steady_clock::now();
    auto result = read(iteration);
    RETURN_ON_ERROR(result);
    if (*result != bytesPerRead) return makeError(CacheCode::kInvalidResponse, "benchmark short read");
    latencies.push_back(std::chrono::duration<double, std::micro>(std::chrono::steady_clock::now() - start).count());
  }
  auto elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - scenarioStart).count();
  std::sort(latencies.begin(), latencies.end());
  auto percentile = [&](size_t numerator) {
    auto index = std::min(latencies.size() - 1, (latencies.size() - 1) * numerator / 100);
    return latencies[index];
  };
  auto totalBytes = iterations * bytesPerRead;
  return ScenarioResult{std::move(name),
                        totalBytes / static_cast<double>(1ULL << 20) / elapsed,
                        percentile(50),
                        percentile(99),
                        originRequests() - originBefore,
                        (hitBytes() - hitBefore) / static_cast<double>(totalBytes)};
}

meta::Inode originInode(uint64_t length, uint32_t blockSize, ImmutableObjectIdentity identity) {
  return meta::Inode{
      meta::InodeId{101},
      meta::InodeData{
          meta::OriginFile{length, meta::Layout::newEmpty(flat::ChainTableId{1}, blockSize, 1), std::move(identity)}}};
}

}  // namespace

Result<std::vector<ScenarioResult>> run() {
  if (FLAGS_iterations == 0 || FLAGS_object_size == 0 || FLAGS_read_size == 0 || FLAGS_block_size == 0 ||
      FLAGS_read_size > FLAGS_object_size || FLAGS_object_size % FLAGS_block_size != 0 ||
      FLAGS_read_size % FLAGS_block_size != 0 || FLAGS_object_size % FLAGS_read_size != 0) {
    return makeError(StatusCode::kInvalidArg,
                     "iterations and sizes must be positive; object/read sizes must be block aligned");
  }

  std::vector<uint8_t> data(FLAGS_object_size);
  for (size_t index = 0; index < data.size(); ++index) data[index] = static_cast<uint8_t>(index);
  auto identity = ImmutableObjectIdentity{OriginId{1},
                                          "benchmark",
                                          "object",
                                          {VersionSelectorType::VERSION_ID, "synthetic-version"}};
  auto inode = originInode(data.size(), FLAGS_block_size, identity);
  auto session = meta::SessionInfo{ClientId::random(), Uuid::random()};
  MemoryObjectStore store(data);
  client::cache::LocalMissSingleflight singleflight;
  client::cache::OriginMissReader missReader(
      store,
      singleflight,
      {.maxRangeBytes = FLAGS_read_size, .maxInflightBytes = FLAGS_read_size, .maxConcurrentRequests = 1});
  std::vector<uint8_t> output(FLAGS_read_size);
  std::vector<ScenarioResult> results;

  uint64_t originBefore = store.requests.load();
  uint64_t hitBefore = 0;
  auto direct = runScenario(
      "cold_direct_s3",
      FLAGS_iterations,
      FLAGS_read_size,
      originBefore,
      hitBefore,
      [&](uint64_t iteration) {
        auto offset = iteration * FLAGS_read_size % FLAGS_object_size;
        return folly::coro::blockingWait(
            missReader.read(identity, data.size(), FLAGS_block_size, offset, std::span<uint8_t>{output}));
      },
      [&] { return store.requests.load(); },
      [] { return uint64_t{0}; });
  RETURN_ON_ERROR(direct);
  results.push_back(*direct);

  for (auto mode : {PlanMode::MISS, PlanMode::WARM, PlanMode::MIXED}) {
    auto source = std::make_shared<BenchmarkPlanSource>(inode, data, FLAGS_block_size, mode);
    client::cache::ReadPlanner planner(source);
    BenchmarkHitReader hitReader(data);
    client::cache::CacheReadPipeline pipeline(missReader, planner, hitReader);
    originBefore = store.requests.load();
    hitBefore = hitReader.hitBytes.load();
    uint64_t originNow = originBefore;
    uint64_t hitNow = hitBefore;
    auto name = mode == PlanMode::MISS ? "foreground_miss" : mode == PlanMode::WARM ? "warm_hit" : "mixed";
    auto scenario = runScenario(
        name,
        FLAGS_iterations,
        FLAGS_read_size,
        originBefore,
        hitBefore,
        [&](uint64_t iteration) {
          auto offset = iteration * FLAGS_read_size % FLAGS_object_size;
          auto result = folly::coro::blockingWait(
              pipeline.read(flat::UserInfo{}, inode, session, offset, std::span<uint8_t>{output}));
          originNow = store.requests.load();
          hitNow = hitReader.hitBytes.load();
          return result;
        },
        [&] { return originNow; },
        [&] { return hitNow; });
    RETURN_ON_ERROR(scenario);
    results.push_back(*scenario);
  }

  uint64_t nativeOrigin = 0;
  uint64_t nativeHits = 0;
  auto native = runScenario(
      "native_3fs",
      FLAGS_iterations,
      FLAGS_read_size,
      0,
      0,
      [&](uint64_t iteration) -> Result<size_t> {
        auto offset = iteration * FLAGS_read_size % FLAGS_object_size;
        std::memcpy(output.data(), data.data() + offset, output.size());
        nativeHits += output.size();
        return output.size();
      },
      [&] { return nativeOrigin; },
      [&] { return nativeHits; });
  RETURN_ON_ERROR(native);
  results.push_back(*native);
  return results;
}

}  // namespace hf3fs::cache::benchmark

int main(int argc, char **argv) {
  folly::init(&argc, &argv, true);
  auto results = hf3fs::cache::benchmark::run();
  if (results.hasError()) {
    std::cerr << results.error().describe() << '\n';
    return EXIT_FAILURE;
  }

  std::ostringstream output;
  output << "scenario,throughput_mib_s,p50_us,p99_us,s3_requests,hit_ratio\n";
  for (const auto &result : *results) {
    output << result.name << ',' << std::fixed << std::setprecision(3) << result.throughputMiB << ',' << result.p50Us
           << ',' << result.p99Us << ',' << result.originRequests << ',' << result.hitRatio << '\n';
  }
  std::cout << output.str();
  if (!FLAGS_output.empty()) {
    std::ofstream file(FLAGS_output, std::ios::trunc);
    if (!file) return EXIT_FAILURE;
    file << output.str();
  }
  return EXIT_SUCCESS;
}
