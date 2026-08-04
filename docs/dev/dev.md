# 基于 3FS 的 AI 数据缓存系统最终开发设计

## 1. 系统目标

在 3FS 基础上实现一套面向 GPU Cluster 的文件缓存系统：

```text
GPU Cluster
    |
3FS File Client / FUSE
    |
3FS Cache
    |
S3-Compatible Object Storage
```

用户通过文件路径访问数据：

```text
/datasets/llama/train-00001.parquet
/models/llama/model-00001.safetensors
```

文件实际来源可能是：

```text
s3://training-data/llama/train-00001.parquet
s3://model-data/llama/model-00001.safetensors
```

3FS 负责缓存数据的高速存储和读取，S3 是权威数据源。

缓存命中时：

```text
Client -> 3FS RDMA -> NVMe
```

缓存未命中时：

```text
Client -> S3 -> 返回用户

Client 同时向 Cache Manager 发送加载提示
Cache Manager 后台把数据写入 3FS
```

---

# 2. 模块划分

需要开发和修改以下模块：

1. Origin-backed File Namespace
2. S3 Origin Adapter
3. Metadata Cache State Extension
4. Cache Chain Table
5. Storage Cache Support
6. Client Read Pipeline
7. FUSE / Native File API
8. Cache Manager Service
9. Cache Admission Policy
10. Prefetch Job
11. Cache Eviction
12. Write-through Pipeline
13. Failure Recovery 与 Reconcile
14. Admin CLI 与可观测性
15. Tests 与 Benchmarks

---

# 3. Origin-backed File Namespace

## 3.1 修改目标

让 3FS 原有 namespace 中的一个普通文件可以表示：

```text
该文件的数据来自一个 S3 对象，
3FS 中的数据只是这个文件的缓存。
```

继续复用 3FS 原有：

* inode
* dentry
* path lookup
* stat
* list
* open
* permission
* FUSE 文件语义

3FS 的 `File` 结构当前已经包含 `length` 和 `Layout`，并且能够根据 inode、offset 和 layout 计算 `ChunkId` 与 `ChainId`。因此 Origin-backed file 应继续使用现有 `Layout`，只扩展文件的数据来源属性。

---

## 3.2 修改文件

修改：

```text
src/fbs/meta/Schema.h
src/fbs/meta/Schema.cc
src/meta/store/Inode.h
src/meta/store/Inode.cc
src/meta/store/ops/Create.*
src/meta/store/ops/SetAttr.*
src/meta/store/MetaStore.*
```

新增：

```text
src/meta/store/ops/ImportOriginFile.*
src/meta/store/ops/RefreshOriginFile.*
src/meta/components/OriginNamespaceManager.*
```

---

## 3.3 数据结构修改

在 `File` 中增加：

```cpp
enum class FileBackingType : uint8_t {
    THREE_FS,
    OBJECT_STORE,
};

struct OriginObjectInfo {
    OriginId originId;

    std::string bucket;
    std::string objectKey;

    std::string versionId;
    std::string etag;

    uint64_t objectSize;
    int64_t lastModifiedNs;

    ObjectStorageClass storageClass;
};

struct File {
    uint64_t length;
    uint64_t truncateVer;
    Layout layout;
    Flags flags;
    uint32_t dynStripe;

    FileBackingType backingType;
    std::optional<OriginObjectInfo> origin;
};
```

Origin-backed file 创建时：

```text
backingType = OBJECT_STORE
length = objectSize
layout.tableId = cache chain table
layout.chunkSize = cache block size
layout.stripeSize = cache stripe size
```

这样原来的：

```cpp
File::getChunkId()
File::getChainId()
```

仍然可以用于计算缓存 block 对应的 3FS Chunk。

---

## 3.4 对象版本更新

对象版本变化时，不直接修改旧 inode，而是：

1. 创建新的 Origin-backed inode；
2. 写入新的 ETag、VersionId、size；
3. 使用相同 path 原子替换 dentry；
4. 已打开旧文件的 Client 继续读取旧 inode；
5. 旧 inode 对应的 cache chunk 进入 GC；
6. 新 inode 会生成新的 ChunkId。

这样：

```text
ChunkId = inode + block index
```

天然带有对象版本隔离。

不需要把 ETag 或 generation 编码到 ChunkId。

---

## 3.5 Namespace 导入接口

新增 Metadata RPC：

```cpp
struct ImportOriginFileReq {
    PathAt path;

    OriginObjectInfo origin;

    ChainTableId cacheChainTable;
    uint32_t cacheBlockSize;
    uint32_t stripeSize;

    Permission permission;
};

struct ImportOriginFileRsp {
    Inode inode;
};
```

批量版本：

```cpp
struct BatchImportOriginFilesReq {
    std::vector<ImportOriginFileEntry> files;
};
```

用于：

* manifest 导入
* S3 prefix 同步
* Cache Manager 创建 dataset namespace

---

## 3.6 完成标准

* S3 对象可以导入成 3FS 文件；
* `stat/open/list/readdir` 使用原 3FS 语义；
* 文件 offset 可以计算对应 ChunkId 和 ChainId；
* 对象版本变化会创建新 inode；
* 旧缓存不会被新文件误命中；
* 原生 3FS 文件行为不受影响。

---

# 4. S3 Origin Adapter

## 4.1 修改目标

为 Client 和 Cache Manager 提供统一对象存储接口。

底层支持：

* AWS S3
* TOS
* MinIO
* OSS S3-compatible
* Ceph RGW

上层不感知对象存储厂商差异。

---

## 4.2 新增目录

```text
src/cache/origin/
    ObjectStore.h
    ObjectStoreFactory.*
    ObjectTypes.h
    OriginError.h

src/cache/origin/s3/
    S3ObjectStore.*
    S3ClientPool.*
    S3CredentialProvider.*
    S3RequestExecutor.*
    S3ErrorMapper.*
    S3Config.h
```

---

## 4.3 核心接口

```cpp
struct ObjectRef {
    OriginId originId;
    std::string bucket;
    std::string key;
};

struct ObjectVersion {
    std::string versionId;
    std::string etag;

    uint64_t size;
    int64_t lastModifiedNs;
};

struct ByteRange {
    uint64_t offset;
    uint64_t length;
};

class ObjectStore {
public:
    virtual CoTryTask<ObjectMetadata>
    headObject(const ObjectRef &object) = 0;

    virtual CoTryTask<size_t>
    readRange(
        const ObjectRef &object,
        const ObjectVersion &expectedVersion,
        ByteRange range,
        std::span<uint8_t> output) = 0;

    virtual CoTryTask<ListResult>
    listObjects(
        const std::string &bucket,
        const std::string &prefix,
        const std::string &continuationToken,
        uint32_t limit) = 0;

    virtual CoTryTask<PutResult>
    putObject(...) = 0;

    virtual CoTryTask<void>
    deleteObject(...) = 0;
};
```

---

## 4.4 版本一致性

`readRange()` 必须携带对象版本。

有 VersionId 时：

```text
GET object?versionId=xxx
```

没有 VersionId 时：

```text
GET object
If-Match: <etag>
```

如果对象在 HEAD 和 GET 之间被覆盖：

```text
返回 OriginError::VersionMismatch
```

调用方重新刷新 namespace 中的对象版本。

---

## 4.5 线程模型

S3 SDK 请求进入独立线程池：

```text
Client Coroutine
    |
OriginRequestExecutor
    |
S3 IO Thread Pool
    |
S3 SDK
```

配置：

```cpp
struct OriginExecutorConfig {
    uint32_t ioThreads;

    uint32_t maxConcurrentRequests;
    uint32_t maxConcurrentRequestsPerOrigin;

    uint64_t maxInflightBytes;

    Duration connectTimeout;
    Duration requestTimeout;

    uint32_t maxRetries;
};
```

S3 请求不能阻塞：

* 3FS RDMA worker
* Metadata executor
* Storage update worker

---

## 4.6 完成标准

* HEAD、Range GET、LIST、PUT、DELETE 正常工作；
* ETag 不匹配时返回 VersionMismatch；
* MinIO 和至少一种实际对象存储通过测试；
* 连接和 S3 Client 可以复用；
* 支持并发与带宽限制；
* Origin 请求不会阻塞 3FS 线程。

---

# 5. Metadata Cache State Extension

## 5.1 修改目标

直接在 3FS Metadata Service 中维护缓存 block 状态。

Client 获取文件布局时，同时获取：

* block index
* ChunkId
* ChainId
* cache state
* checksum

不增加额外 Cache Metadata RPC 服务。

3FS Metadata 的入口目前集中在 `MetaOperator`，新增缓存 RPC 应继续在这里注册和实现。

---

## 5.2 修改文件

修改：

```text
src/fbs/meta/Common.h
src/fbs/meta/Schema.h
src/fbs/meta/Service.h

src/meta/service/MetaOperator.h
src/meta/service/MetaOperator.cc
src/meta/service/MetaSerdeService.h

src/meta/store/MetaStore.h
src/meta/store/MetaStore.cc

src/client/meta/MetaClient.h
src/client/meta/MetaClient.cc
```

新增：

```text
src/meta/store/cache/CacheBlockRecord.*
src/meta/store/cache/CacheBlockStore.*
src/meta/store/ops/GetReadPlan.*
src/meta/store/ops/EnqueueCacheBlocks.*
src/meta/store/ops/AcquireCacheBlocks.*
src/meta/store/ops/CommitCacheBlocks.*
src/meta/store/ops/BeginEvictCacheBlocks.*
src/meta/store/ops/ReportCacheEvents.*
```

---

## 5.3 Cache Block Key

```cpp
struct CacheBlockKey {
    InodeId inode;
    uint32_t blockIndex;
};
```

ChunkId 使用现有规则：

```cpp
ChunkId chunkId(inode, 0, blockIndex);
```

3FS 当前 ChunkId 已经编码 inode、track 和 chunk number，适合直接把一个文件的 cache block 映射成一个 ChunkId。

---

## 5.4 Cache 状态

```cpp
enum class CacheBlockState : uint8_t {
    QUEUED,
    LOADING,
    READY,
    EVICTING,
    FAILED,
    INVALID,
};
```

不存在记录表示：

```text
NONE
```

状态机：

```text
NONE
  |
  v
QUEUED
  |
  v
LOADING
  | \
  |  \ failure
  |   v
  | FAILED
  |
  v
READY
  |
  v
EVICTING
  |
  v
NONE
```

Storage 丢失或 emergency eviction：

```text
READY -> INVALID -> NONE
```

---

## 5.5 Cache Block Record

```cpp
struct CacheBlockRecord {
    InodeId inode;
    uint32_t blockIndex;

    CacheBlockState state;

    ChainId chainId;
    ChecksumInfo checksum;
    uint32_t blockLength;

    LoaderId loaderId;
    uint64_t loadEpoch;
    int64_t leaseExpireNs;

    uint32_t priority;
    uint32_t pinRefCount;

    int64_t createdAtNs;
    int64_t readyAtNs;
    int64_t lastAccessNs;

    uint64_t accessCount;

    uint32_t retryCount;
    CacheError lastError;
};
```

`loadEpoch` 是 fencing token。

每次从 QUEUED 进入 LOADING：

```text
loadEpoch++
```

Loader Commit 时必须携带相同 epoch，防止旧 Loader lease 过期后提交错误结果。

---

## 5.6 FDB Key

```text
/cache/block/{inode}/{block-index}
```

Value：

```text
CacheBlockRecord
```

访问索引：

```text
/cache/evict/{priority}/{last-access}/{inode}/{block-index}
```

Pin：

```text
/cache/pin/{job-id}/{inode}/{begin-block}/{end-block}
```

任务：

```text
/cache/job/{job-id}
```

---

## 5.7 GetFileReadPlan RPC

新增：

```cpp
struct GetFileReadPlanReq : ReqBase {
    InodeId inode;
    uint64_t offset;
    uint64_t length;
};

struct ReadBlockPlan {
    uint32_t blockIndex;

    uint64_t fileOffset;
    uint32_t blockLength;

    CacheStateView cacheState;

    ChunkId chunkId;
    ChainId chainId;

    ChecksumInfo checksum;

    uint64_t originOffset;
};

struct GetFileReadPlanRsp : RspBase {
    Inode inode;
    OriginObjectInfo origin;

    std::vector<ReadBlockPlan> blocks;
};
```

处理过程：

1. 加载 inode；
2. 检查为 Origin-backed file；
3. 根据 `layout.chunkSize` 计算 block 范围；
4. 批量读取 `/cache/block`；
5. 没有记录返回 NONE；
6. 使用现有 `File::getChunkId()` 和 `getChainId()` 计算 Chunk 和 Chain；
7. 一次返回给 Client。

---

## 5.8 Cache Manager 使用的 RPC

### EnqueueCacheBlocks

```cpp
BatchEnqueueCacheBlocksReq
BatchEnqueueCacheBlocksRsp
```

转换：

```text
NONE / FAILED / INVALID -> QUEUED
```

### AcquireCacheBlocks

```text
QUEUED -> LOADING
```

写入：

* loaderId
* loadEpoch
* leaseExpireNs

### CommitCacheBlocks

验证：

* state == LOADING
* loaderId 匹配
* loadEpoch 匹配
* inode 仍然存在

转换：

```text
LOADING -> READY
```

### FailCacheBlocks

```text
LOADING -> FAILED
```

### BeginEvictCacheBlocks

```text
READY -> EVICTING
```

Pin block 不允许进入 EVICTING。

### ReportCacheEvents

接收 Storage Event：

* DELETED
* EMERGENCY_EVICTED
* LOST
* CORRUPTED

---

## 5.9 完成标准

* GetFileReadPlan 一次返回文件布局与缓存状态；
* 没有缓存的 block 不创建记录；
* 多个 Loader 只能有一个获得 LOADING；
* 旧 loadEpoch 不能 Commit；
* Storage Event 可以幂等处理；
* 对象 inode 被替换后旧 Loader 无法写入新文件；
* 批量查询 100～1000 个 block 性能满足读路径。

---

# 6. Cache Chain Table

## 6.1 修改目标

通过独立 Chain Table 隔离缓存数据和普通 3FS 数据。

3FS 当前 `Layout` 已包含 `tableId`、`tableVersion`、`chunkSize` 和 chain 分配信息，`ChainAllocator` 已经根据 Layout 指定的 Chain Table 分配 Chain。因此缓存文件只需使用独立 tableId，不需要新建一套 Chain 管理系统。

---

## 6.2 修改文件

修改：

```text
src/fbs/mgmtd/MgmtdTypes.h
src/fbs/mgmtd/ChainTable.h
src/fbs/mgmtd/RoutingInfo.h

src/mgmtd/store/MgmtdStore.*
src/mgmtd/ops/SetChainTableOperation.*
src/mgmtd/service/RoutingInfo.*

src/client/mgmtd/MgmtdClient.*
src/client/cli/admin/UploadChainTable.cc
src/client/cli/admin/DumpChainTable.cc
src/client/cli/admin/ListChainTables.cc
```

3FS 当前已经具备 Chain Table 的上传、设置、列举和路由能力，这些路径作为缓存 Chain Table 的基础。

---

## 6.3 Chain Table 属性扩展

```cpp
enum class ChainTableRole : uint8_t {
    USER_DATA,
    CACHE_DATA,
    WRITE_STAGING,
};

struct ChainTableAttributes {
    ChainTableRole role;

    uint64_t logicalCapacity;

    double highWatermark;
    double lowWatermark;
    double emergencyWatermark;

    uint32_t defaultPriority;

    bool emergencyEvictable;
};
```

缓存 Chain Table 配置示例：

```text
role = CACHE_DATA
replica count = 1
logical capacity = 500 TB
high watermark = 85%
low watermark = 75%
emergency watermark = 95%
emergency evictable = true
```

---

## 6.4 Chain 分配

Origin-backed file 创建时：

```cpp
Layout layout = Layout::newEmpty(
    cacheChainTableId,
    cacheBlockSize,
    stripeSize);
```

继续调用：

```cpp
ChainAllocator::allocateChainsForLayout(layout);
```

Metadata 不新增 CacheChainAllocator。

Cache Chain Table 的具体 Chain 配置由 mgmtd 管理。

---

## 6.5 完成标准

* 可以创建和列举 CACHE_DATA Chain Table；
* Origin-backed file 只能使用 CACHE_DATA table；
* 普通文件继续使用 USER_DATA table；
* Cache Chain Table 的容量和水位可以查询；
* Storage 可以根据 ChainId 判断是否属于缓存；
* 缓存容量不会占用普通数据的 Chain Table 配额。

---

# 7. Storage Cache Support

## 7.1 修改目标

复用现有 StorageClient、StorageOperator 和 ChunkEngine：

* batchRead
* batchWrite
* removeChunks
* checksum
* routing
* RDMA
* failure handling

3FS `StorageClient` 已经提供 chunk 级 `batchRead`、`batchWrite`、`read`、`write`、`removeChunks` 和 `queryChunk` 等接口，可直接作为缓存数据路径。

---

## 7.2 修改文件

修改：

```text
src/fbs/storage/Common.h
src/fbs/storage/Service.h

src/storage/service/StorageOperator.h
src/storage/service/StorageOperator.cc

src/storage/store/ChunkEngine.h
src/storage/store/ChunkEngine.cc
src/storage/store/StorageTarget.*
src/storage/store/StorageTargets.*

src/client/storage/StorageClient.h
src/client/storage/StorageClientImpl.*

src/storage/Components.*
```

新增：

```text
src/storage/cache/CacheChainClassifier.*
src/storage/cache/CacheEventReporter.*
src/storage/cache/CacheEventJournal.*
src/storage/cache/EmergencyEvictor.*
```

---

## 7.3 Cache Chain 分类

Storage 收到请求时，通过：

```text
ChainId -> ChainTableRole
```

判断是否为缓存 Chunk。

新增：

```cpp
bool CacheChainClassifier::isCacheChain(ChainId chain);
```

Storage 不需要修改 ChunkEngine 的物理 key。

ChunkEngine 当前使用 `ChainId + ChunkId` 构造物理 key，可以继续直接存放 cache chunk。

---

## 7.4 Batch Remove

增加离散批量删除：

```cpp
struct BatchRemoveChunksReq {
    std::vector<ChunkKey> chunks;
};

struct ChunkKey {
    ChainId chainId;
    ChunkId chunkId;
};
```

修改：

```text
StorageClient::MethodType
StorageClient
StorageClientImpl
StorageOperator
ChunkEngine
```

实现过程：

1. 按 Node/Target 分组；
2. 并发发送；
3. 每个 Chunk 返回独立结果；
4. 删除成功后写入 Cache Event Journal；
5. 异步发送 DELETED event。

---

## 7.5 Cache Event

```cpp
enum class CacheStorageEventType {
    DELETED,
    EMERGENCY_EVICTED,
    LOST,
    CORRUPTED,
};

struct CacheStorageEvent {
    Uuid eventId;

    CacheStorageEventType type;

    NodeId nodeId;
    TargetId targetId;

    ChainId chainId;
    ChunkId chunkId;

    uint64_t sequence;
    int64_t timestampNs;
};
```

事件投递：

```text
Storage
    |
CacheEventJournal
    |
Batch Report RPC
    |
Metadata
    |
ACK sequence
```

事件至少投递一次。

Metadata 必须幂等处理。

---

## 7.6 Emergency Eviction

每个 Storage Target 维护：

```cpp
emergencyWatermark
```

达到阈值后：

1. 只遍历 cache chain；
2. 选择本地缓存 chunk；
3. 删除 chunk；
4. 记录 EMERGENCY_EVICTED event；
5. 上报 Metadata。

第一版 victim 可以使用：

* 最老创建时间
* 随机采样最老
* 本地近似 LRU

正常淘汰仍由 Cache Manager 决定。

---

## 7.7 Query Cache Inventory

Reconciler 需要查询 Cache Chunk：

```cpp
struct QueryCacheInventoryReq {
    ChainTableId chainTable;
    TargetId target;
    std::optional<ChunkId> cursor;
    uint32_t limit;
};

struct QueryCacheInventoryRsp {
    std::vector<ChunkMetadata> chunks;
    std::optional<ChunkId> nextCursor;
};
```

可以复用和封装 ChunkEngine 当前的查询接口。

---

## 7.8 完成标准

* Cache chunk 可以 batch write 和 batch read；
* Cache Manager 可以离散批量删除；
* 正常删除后 Metadata 收到 DELETED event；
* Emergency eviction 只处理 cache chain；
* Event 在网络故障后可以重试；
* Reconciler 可以分页扫描实际缓存 Chunk；
* 原生 3FS Storage 功能不受影响。

---

# 8. Client Read Pipeline

## 8.1 修改目标

对用户提供文件 read/pread 语义。

Cache hit：

```text
Metadata -> 3FS BatchRead -> 用户
```

Cache miss：

```text
Metadata -> S3 Range GET -> 用户
                   |
                   +-> EnsureCached Hint
```

Client 不负责把 miss 数据写入 3FS。

---

## 8.2 新增目录

```text
src/client/cache/
    CacheReadPipeline.*
    ReadPlanner.*
    ReadPlanCache.*
    CacheHitReader.*
    OriginMissReader.*
    BufferAssembler.*
    OriginRangePlanner.*
    EnsureCachedReporter.*
    AccessReporter.*
    LocalMissSingleflight.*
```

修改：

```text
src/client/meta/MetaClient.*
src/client/storage/StorageClient.*
src/fuse/FuseClients.*
src/fuse/FuseOps.cc
src/fuse/PioV.*
src/fuse/FuseConfig.h
```

---

## 8.3 Read Planner

输入：

```cpp
struct FileReadRequest {
    InodeId inode;

    uint64_t offset;
    uint64_t length;

    std::span<uint8_t> output;
};
```

调用：

```cpp
MetaClient::getFileReadPlan()
```

将 block 分类为：

```text
READY
MISS
LOADING
INVALID
```

READY：

```text
进入 CacheHitReader
```

其他状态：

```text
进入 OriginMissReader
```

---

## 8.4 Cache Hit Reader

把所有 READY block 转换成：

```cpp
storage::client::ReadIO
```

然后一次调用：

```cpp
StorageClient::batchRead()
```

处理：

* block 内偏移
* 最后一个不完整 block
* 多 Chain 路由
* checksum
* partial failure

如果某个 chunk 返回 NotFound：

1. 将该 segment 转为 Origin miss；
2. 删除本地 read plan cache；
3. 调用 Metadata `reportChunkMissing()`；
4. 发送 EnsureCached hint。

---

## 8.5 Origin Miss Reader

对连续 miss block 合并：

```text
block 10
block 11
block 12
```

转换为：

```text
一次 Origin Range GET
```

限制：

```cpp
maxOriginRangeBytes
maxParallelOriginReads
```

返回内容直接写入用户 output buffer。

Client 不等待 Cache Manager 加载。

---

## 8.6 EnsureCached Hint

```cpp
struct EnsureCachedHint {
    InodeId inode;

    uint32_t beginBlock;
    uint32_t blockCount;

    JobId jobId;

    AccessReason reason;
    AccessPattern pattern;

    uint32_t priority;
};
```

调用方式：

```text
fire-and-forget
短超时
失败不影响用户 read
```

---

## 8.7 Read Plan Cache

Client 本地缓存：

```cpp
struct CachedReadPlan {
    InodeId inode;
    uint64_t inodeVersion;

    OriginObjectInfo origin;
    Layout layout;

    std::unordered_map<uint32_t, CachedBlockPlan> blocks;

    int64_t expireAt;
};
```

READY block TTL 较短。

Origin inode 被替换后，新的 inodeId 会天然使缓存失效。

---

## 8.8 本地 Miss Singleflight

相同 Client 内多个线程读取同一 Origin Range：

```text
只执行一次 S3 GET
其他线程等待同一个 Future
```

Key：

```cpp
OriginId + bucket + key + version + offset + length
```

---

## 8.9 完成标准

* 全命中时不访问 S3 和 Cache Manager；
* 全 miss 时直接从 S3 返回；
* mixed hit/miss 返回内容正确；
* Client 不写入 3FS；
* 相邻 miss Range 会合并；
* READY 但 Chunk 丢失时可以 fallback；
* 同进程相同 miss 不重复访问 S3；
* hit 路径性能接近原生 3FS read。

---

# 9. FUSE 与 Native File API

## 9.1 修改目标

在不改变用户文件接口的情况下，为 Origin-backed file 选择 CacheReadPipeline。

---

## 9.2 修改文件

```text
src/fuse/FuseOps.cc
src/fuse/PioV.cc
src/fuse/PioV.h
src/fuse/FuseClients.cc
src/fuse/FuseClients.h
src/fuse/FuseConfig.h
```

3FS 当前 FUSE 入口集中在 `FuseOps.cc` 和 `PioV`，缓存读路径应从这些位置接入。

---

## 9.3 Read 分流

打开文件后保存：

```cpp
struct OpenFileContext {
    Inode inode;

    bool originBacked;

    std::shared_ptr<CacheReadPipeline> cachePipeline;
};
```

read/pread：

```cpp
if (inode.file.backingType == OBJECT_STORE) {
    cachePipeline->read(...);
} else {
    existingThreeFSRead(...);
}
```

---

## 9.4 写语义分流

Origin-backed file 的写入进入 Write-through Pipeline。

原生 3FS 文件继续使用现有写路径。

---

## 9.5 完成标准

* FUSE 用户通过普通 path 访问 Origin 文件；
* open/stat/readdir 保持文件语义；
* read 自动使用 cache；
* Native File API 和 FUSE 使用相同 CacheReadPipeline；
* 原生 3FS 文件性能和语义不变。

---

# 10. Cache Manager Service

## 10.1 修改目标

新增独立服务，负责所有后台缓存控制：

* Client EnsureCached Hint
* 用户 Prefetch Job
* Admission
* Loader
* Pin
* Eviction
* Reconcile

Cache Manager 不参与 cache hit 快路径。

---

## 10.2 新增目录

```text
src/cache_manager/
    app/
    service/
    client/
    scheduler/
    planner/
    loader/
    admission/
    prefetch/
    eviction/
    access/
    reconcile/
    config/
```

新增协议：

```text
src/fbs/cache_manager/Common.h
src/fbs/cache_manager/Service.h
```

---

## 10.3 服务 API

```cpp
EnsureCached(EnsureCachedReq)
CreatePrefetchJob(CreatePrefetchJobReq)
GetPrefetchJob(GetPrefetchJobReq)
CancelPrefetchJob(CancelPrefetchJobReq)
PinDataset(PinDatasetReq)
UnpinDataset(UnpinDatasetReq)
GetCacheStatus(GetCacheStatusReq)
TriggerEviction(TriggerEvictionReq)
```

---

## 10.4 EnsureCached 处理

收到 Client Hint：

1. 合并相同 inode + block range；
2. 查询 Metadata 状态；
3. 调用 Admission Policy；
4. BYPASS：结束；
5. CACHE：调用 Metadata `EnqueueCacheBlocks`；
6. 加入 Loader Scheduler。

API 返回：

```cpp
enum EnsureCachedResult {
    ACCEPTED,
    BYPASSED,
    ALREADY_READY,
    ALREADY_LOADING,
};
```

---

## 10.5 Loader Scheduler

维护优先级队列：

```text
P0: model/checkpoint explicit prefetch
P1: high-priority job prefetch
P2: repeated foreground miss
P3: sequential prefetch
P4: speculative load
```

限制：

* 每个 Origin 并发
* 全局 S3 带宽
* 3FS 写入并发
* inflight bytes
* 每个 Job 配额

---

## 10.6 Cache Loader

Loader 执行：

1. Metadata `AcquireCacheBlocks`；
2. 按对象和连续 block 合并；
3. S3 Range GET；
4. 拆成 cache block；
5. 生成 ChunkId 和 ChainId；
6. `StorageClient::batchWrite()`；
7. Metadata `CommitCacheBlocks()`；
8. 失败时 `FailCacheBlocks()`。

建议：

```text
cache block = 16 MiB
origin fetch range = 64～256 MiB
storage write batch = 受 StorageClient max_batch_bytes 限制
```

---

## 10.7 完成标准

* Client Hint 能触发后台加载；
* 同一 block 不会重复加载；
* Loader 可以合并连续 S3 Range；
* Loader 支持带宽和并发限制；
* 写入后 Metadata 状态变为 READY；
* Cache Manager 重启后任务可以恢复；
* Cache Manager 宕机不影响 cache hit 和 foreground miss。

---

# 11. Cache Admission Policy

## 11.1 修改目标

决定一个 block 是否进入 3FS Cache。

---

## 11.2 新增文件

```text
src/cache_manager/admission/AdmissionPolicy.h
src/cache_manager/admission/RuleAdmissionPolicy.*
src/cache_manager/admission/FrequencyAdmissionPolicy.*
src/cache_manager/admission/AdmissionContext.*
```

---

## 11.3 输入

```cpp
struct AdmissionContext {
    InodeId inode;

    DatasetType datasetType;
    JobPriority jobPriority;

    uint64_t fileSize;
    uint32_t blockCount;

    uint64_t recentMissCount;
    double estimatedReuse;

    double cachePoolUsage;
    double targetMaxUsage;

    AdmissionReason reason;

    bool explicitlyRequested;
    bool pinRequested;
};
```

---

## 11.4 第一版规则

顺序执行：

1. 显式 Prefetch Job：

   ```text
   CACHE
   ```

2. Pin 请求：

   ```text
   CACHE_AND_PIN
   ```

3. 模型权重、checkpoint：

   ```text
   CACHE
   ```

4. 标记为临时数据：

   ```text
   BYPASS
   ```

5. Cache 超过 high watermark：

   ```text
   只允许高优先级 CACHE
   ```

6. 普通 Client miss：

   ```text
   第一次 BYPASS
   短窗口内第二次 CACHE
   ```

---

## 11.5 输出

```cpp
struct AdmissionDecision {
    AdmissionAction action;

    uint32_t priority;
    Duration ttl;

    std::string reason;
};
```

每次决策必须记录 reason。

---

## 11.6 完成标准

* 显式预取始终准入；
* 临时数据可旁路；
* 高频数据在第二次或达到阈值后准入；
* 高水位时低优先级请求不进入 cache；
* Admission 延迟不会影响 foreground Client read。

---

# 12. Prefetch Job

## 12.1 修改目标

允许用户在 GPU Job 启动前提交数据预取任务。

输入可以是：

* 3FS namespace path
* 文件列表
* manifest
* S3 prefix
* checkpoint path

---

## 12.2 数据结构

```cpp
enum class PrefetchJobState {
    PENDING,
    PLANNING,
    LOADING,
    PARTIAL_READY,
    READY,
    FAILED,
    CANCELLED,
};

struct PrefetchJobSpec {
    JobId jobId;

    std::vector<DatasetSource> sources;

    uint32_t priority;

    uint32_t maxParallelLoads;
    uint64_t bandwidthLimit;

    double requiredReadyRatio;

    bool pinAfterReady;
    Duration pinTtl;
};
```

---

## 12.3 Planner

Planner 负责：

1. 展开 namespace path；
2. 加载 manifest；
3. 通过 S3 ListObjects 展开 prefix；
4. 获取文件 inode；
5. 按 file layout 计算 block ran`PlannedBlockRange`。

```cpp
struct PlannedBlockRange {
    InodeId inode;

    uint32_t beginBlock;
    uint32_t blockCount;

    uint32_t priority;
};
```

---

## 12.4 READY 条件

```text
readyRatio = readyBytes / plannedBytes
```

达到：

```text
requiredReadyRatio
```

任务进入 READY。

用户或调度器可以在 READY 后启动 GPU Job。

---

## 12.5 完成标准

* 可以提交、查询、取消任务；
* path、manifest、prefix 均可生成 block plan；
* Ready Ratio 正确；
* Pi成后生效；
* 高优先级任务可以先执行；
* 任务取消后未开始的 block 不再加载。

---

# 13. Cache Eviction

## 13.1 修改目标

在缓存达到高水位后主动释放空间。

---

## 13.2 新增文件

```text
src/cache_manager/eviction/EvictionController.*
src/cache_manager/eviction/EvictionCandidateSelector.*
src/cache_manager/eviction/EvictionPolicy.*
src/cache_manager/access/AccessAggregator.*
```

---

## 13.3 Access 上报

Client 按采样上报：

```cpp
struct Cache    InodeId inode;
    uint32_t blockIndex;

    uint64_t hitCount;
    uint64_t bytes;

    int64_t lastAccessNs;
};
```

Cache Manager 聚合后批量写入 Metadata。

不在每次 read 时同步修改 Metadata。

---

## 13.4 候选选择

第一版算法：

1. 排除 pinned；
2. priority 从低到高；
3. 同 priority 按 lastAccess 从旧到新；
4. 优先选择连续 block；
5. 删除到 low watermark。

Metadata eviction index：

```text
/cache/evict/{priority}/{last-access}/{inode}/{block--

## 13.5 删除流程

```text
READY
    |
BeginEvict
    |
EVICTING
    |
Storage BatchRemove
    |
Storage DELETED Event
    |
删除 CacheBlockRecord
```

Client 如果在 EVICTING 期间持有旧 ReadPlan：

```text
3FS NotFound
    ->
fallback S3
```

---

## 13.6 完成标准

* 高水位触发 eviction；
* 删除后降到低水位；
* pinned 数据不会删除；
* 低优先级数据先删除；
* Storage 实际空间正确下降；
* 读和淘汰并发时用户读取正确。

---

# 14. Write-ugh Pipeline

## 14.1 修改目标

为 checkpoint 等场景提供文件写入语义，并最终持久化到 S3。

---

## 14.2 Staging Chain Table

新增：

```text
WRITE_STAGING Chain Table
```

配置较高副本数，用于写入尚未持久化到 S3 的数据。

---

## 14.3 写流程

1. 用户 create 新文件；
2. 创建普通 staging inode；
3. Client 使用现有 3FS write pipeline 写 staging chain；
4. close/fsync 创建 Upload Job；
5. Upload Worker 从 3FS 读取；
6. Multipart UploadS3 完成后获取 ETag/VersionId；
8. 创建新的 Origin-backed inode；
9. 原子替换 path 对应 dentry；
10. 删除 staging inode；
11. 提交高优先级 cache prefetch，使新文件进入 Cache Chain。

第一版 write-through：

```text
close/fsync 在 S3 上传成功后返回
```

---

## 14.4 修改文件

修改：

```text
src/meta/store/ops/Create.*
src/meta/store/ops/Sync.*
src/meta/store/ops/Close.*
src/fuse/FuseOps.cc
```

新增：

```text
src/cache_manager/upload/UploadJob.*
src/che_manager/upload/MultipartUploader.*
src/cache_manager/upload/WritePublishController.*
```

---

## 14.5 完成标准

* 顺序写文件可以持久化到 S3；
* close 返回后对象一定存在；
* 新文件可以重新通过文件路径读取；
* 上传失败不会发布不完整文件；
* staging inode 可以恢复和清理。

---

# 15. Failure Recovery 与 Reconcile

## 15.1 修改目标

保证 Metadata 状态、3FS Cache Chunk 和 S3 对象最终收敛。

---

## 15.2 Loader Lease

LOADING ç```cpp
loaderId
loadEpoch
leaseExpireNs
```

Lease 到期：

```text
LOADING -> FAILED -> QUEUED
```

新 Loader 获得更高 loadEpoch。

旧 Loader 无法 Commit。

---

## 15.3 Orphan Chunk

情况：

```text
3FS write 成功
Metadata Commit 失败
```

处理：

1. Loader 主动删除；
2. Reconciler 扫描 Cache Inventory；
3. 没有 READY/LOADING record 的 Chunk 删除。

---

## 15.4 READY 但 Chunk 丢失

发现方式：

* Storage Event
* Client NotFound
* Reconciler

处理：

```text
READY -> INVALID -> NONE
```

然后允许重新加载。

---

## 15.5 Reconciler

新增：

```text
src/cache_manager/reconcile/CacheReconciler.*
src/cache_manager/reconcile/MetadataToStorageChecker.*
src/cache_manager/reconcile/StorageToMetadataChecker.*
src/cache_manager/reconcile/LeaseRecovery.*
```

工作：

### Metadata -> Storage

扫描 READY block，检查 Chunk 是否存在。

### Storage -> Metadata

扫描 Cache Chain Inventory，删除 orphan。

### State Recovery

处理：

* LOADING le过期
* EVICTING 超时
* FAILED 重试
* Pin TTL 到期
* 未完成 Job 恢复

---

## 15.6 完成标准

注入以下故障后系统可以恢复：

* Loader 在 S3 GET 中退出；
* Loader 在 3FS write 后退出；
* Metadata Commit 超时；
* Storage Chunk 被删除；
* Storage Event 重复或延迟；
* Cache Manager 重启；
* S3 429、500、timeout；
* 对象版本在加载过程中变化。

---

# 16. Admin CLI 与可观测性

## 16.1 新增命令

修改：

```text
src/client/bin/admin_cclient/cli/admin/
src/tools/admin.cc
src/tools/commands/Commands.h
```

新增：

```text
cache-import
cache-refresh-origin
cache-create-chain-table
cache-status
cache-prefetch
cache-job-status
cache-job-cancel
cache-pin
cache-unpin
cache-evict
cache-list-blocks
cache-reconcile
cache-dump-events
```

---

## 16.2 指标

### Client

```text
cache_read_bytes
cache_hit_bytes
cache_miss_bytes
cache_hit_ratio
origin_read_bytes
get_read_plan_latency
3fs_read_latency
origin_read_latency
ensure_cached_hints
```

ta

```text
cache_records
cache_state_transition
get_read_plan_latency
load_epoch_conflict
lease_expired
storage_event_delay
```

### Cache Manager

```text
loader_inflight
loader_bytes
admission_cache
admission_bypass
prefetch_ready_ratio
evicted_bytes
pinned_bytes
reconcile_repairs
```

### Storage

```text
cache_chain_used_bytes
cache_chain_read_bytes
cache_chain_write_bytes
normal_eviction_bytes
emergency_eviction_bytes
cache_event_backlog
```

---

# 17. Tests 与 Benchmarks

## 17.1 Metadata Tests

æ```text
tests/meta/cache/
```

测试：

* Origin-backed inode
* GetFileReadPlan
* Enqueue/Acquire/Commit
* loadEpoch fencing
* eviction state
* Storage Event 幂等
* inode 替换与旧 cache 隔离

---

## 17.2 Storage Tests

新增：

```text
tests/storage/cache/
```

测试：

* cache chain 分类
* batch remove
* emergency eviction
* event journal
* inventory scan
* user chain 不受影响

---

## 17.3 Client Tests

新增：

```text
tests/client/cache/
```

测试：

* 全 hit
* 全 miss
* mixrange merge
* local singleflight
* READY but NotFound fallback
* EnsureCached 超时不影响读

---

## 17.4 Cache Manager Tests

新增：

```text
tests/cache_manager/
```

测试：

* admission
* loader dedup
* bandwidth limit
* prefetch job
* pin
* eviction
* restart recovery
* reconcile

---

## 17.5 Benchmark

新增：

```text
benchmarks/cache_bench/
```

场景：

1. 原生 S3 cold read；
2. Cache miss read；
3. 3FS cache hit；
4. 第一次 epoch；
5. 第二次 epoch；
6. checkpoint restor读取；
8. warmup 前后对比。

核心指标：

```text
GB/s
P50/P99 latency
GPU data wait time
S3 bandwidth
cache hit ratio
metadata RPC overhead
CPU usage
```

---

# 18. 最终端到端流程

## 18.1 第一次读取

```text
User read
    |
GetFileReadPlan
    |
Cache NONE
    |
Client direct S3 Range GET
    |
Return User
    |
EnsureCached Hint
    |
Cache Manager Admission
    |
Loader S3 Range GET
    |
3FS BatchWrite
    |
Metadata READY
```

---

## 18.2 第二次读取

```text
User read
  eadPlan
    |
Cache READY
    |
3FS BatchRead
    |
Return User
```

---

## 18.3 Prefetch

```text
User submits Prefetch Job
    |
Planner generates block ranges
    |
Loader writes 3FS
    |
Metadata READY
    |
Ready Ratio reached
    |
GPU Job starts
```

---

## 18.4 Eviction

```text
Cache Pool high watermark
    |
Select victim
    |
Metadata EVICTING
    |
Storage BatchRemove
    |
Storage Event
    |
Metadata remove record
```

---

## 18.5 写入

```text
User write
    |
3FS Staging
    |
close/f
    |
Multipart Upload S3
    |
Publish Origin-backed inode
    |
Prefetch into Cache Chain
```

---

# 19. 推荐实现顺序

## 第一阶段：只读闭环

1. Origin-backed File Schema
2. S3 Origin Adapter
3. Cache Metadata Record
4. GetFileReadPlan
5. Cache Chain Table
6. Client Hit/Miss Read
7. Cache Manager EnsureCached
8. Loader Commit READY

## 第二阶段：容量管理

1. Access Report
2. Admission
3. Eviction
4. Storage Event
5. Emergency Eviction

## 第三阶段：数据编排

1. Prefetch b
2. Manifest/Prefix Planner
3. Ready Ratio
4. Pin
5. Job Priority

## 第四阶段：可靠性与写入

1. Loader Lease
2. Reconciler
3. Cache Manager Recovery
4. Write Staging
5. S3 Multipart Upload
6. Origin-backed inode publish
