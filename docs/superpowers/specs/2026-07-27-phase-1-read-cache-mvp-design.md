# 3FS Cache 第一阶段只读缓存 MVP 设计

## 1. 目标

第一阶段交付一个可试用的只读缓存 MVP。用户将明确指定的 S3 对象导入 3FS namespace 后，继续使用普通文件路径读取数据：首次缓存未命中时由 Client 直接从 S3 返回数据，同时异步提示 Cache Manager；Cache Manager 后台将数据写入 3FS；后续读取命中缓存并通过现有 3FS Storage 数据路径返回。

端到端流程如下：

```text
显式导入 S3 对象
  → 通过普通 3FS 路径读取
  → miss 直接回源并返回
  → 后台加载到 3FS
  → 后续读取命中 3FS
```

本阶段选择可用 MVP，而不是技术原型或准生产版本。支持 MinIO 自动化集成测试和 AWS S3 兼容性验证；支持显式单文件和批量文件映射；每次 foreground miss 默认触发后台缓存，容量达到上限后停止新准入。

第一阶段以功能正确性和可用性为交付门槛。保留基础指标和 benchmark 脚本，但不设置吞吐、延迟或资源消耗的硬性验收指标，也不在本阶段安排专项性能优化。

## 2. 范围

### 2.1 包含

- fail-closed 的独立 `OriginFile` inode variant，以及 namespace 导入、刷新。
- MinIO 与 AWS S3 的 HEAD、版本约束 Range GET。
- Metadata Cache Block Record、状态转换和 Read Plan。
- 独立的 `CACHE_DATA` Chain Table。
- Client 全 miss、全 hit、mixed hit/miss 读取。
- Client 本地 Range 合并和 miss singleflight。
- Cache Manager 的 Hint、准入、调度和 Loader。
- FUSE 和 Native API 共享 CacheReadPipeline。
- 最小 lease 超时回收和 READY 异常上报。
- 基础 Admin CLI、指标、测试与非阻塞 benchmark。

### 2.2 不包含

- 自动 eviction、低水位回收和复杂容量策略。
- Storage Event Journal、emergency eviction 和 inventory scan。
- 完整 Reconcile 和 Cache Manager 持久化任务恢复。
- Prefetch Job、manifest、S3 prefix planner 和 Pin。
- 基于访问频率的准入策略。
- Write-through、WRITE_STAGING、Multipart Upload 和 Origin 文件写入。
- 多 Cache Manager 高可用。
- 专项性能调优和性能硬门槛。

Origin-backed file 在第一阶段是只读文件。write、truncate 和任何改变 length 的操作必须返回明确的只读错误。容量达到配置上限后，已有 READY 数据仍可读取，foreground miss 仍可直接回源，但新的后台加载被拒绝；系统不自动选择或删除 victim。

## 3. 设计原则

1. S3 是权威数据源，3FS chunk 是可重建缓存。
2. Client miss 直接回源，不等待 Cache Manager 填充。
3. Cache Manager 不参与 cache hit 快路径。
4. Metadata 是缓存逻辑状态的权威来源，Storage 保存实际数据。
5. 对象版本变化通过新 inode 隔离，新旧缓存不得互相命中。
6. 缓存和普通 3FS 数据使用不同 Chain Table。
7. FUSE 与 Native API 共用一套 CacheReadPipeline。
8. 普通 3FS 文件继续使用原读写路径，不增加 Cache RPC。
9. 不允许旧二进制把 Origin 文件降级解释成普通 3FS 文件；协议升级必须 fail-closed。

## 4. 组件与职责

### 4.1 Origin Namespace

Origin Namespace 将显式指定的对象映射成具有普通文件路径语义的 Origin 文件，并保存对象身份与版本。Schema 使用独立的 `OriginFile` inode variant，而不是在现有 `File` 尾部追加可被旧客户端忽略的字段。`OriginFile` 复用 Layout、length 和 ChunkId/ChainId 计算帮助函数，但旧客户端或旧 Metadata writer 遇到未知 variant 必须拒绝处理，不能将其当成普通 3FS 文件。

第一阶段提供：

```cpp
ImportOriginFile(path, origin, layout)
BatchImportOriginFiles(files)
RefreshOriginFile(path, expectedInode, newOrigin)
```

导入前通过 ObjectStore HEAD 获取对象 size、ETag 和可选 VersionId。导入操作只创建 namespace，不主动下载对象。path 已存在普通文件时默认拒绝覆盖；已存在同版本 Origin 文件时幂等返回；不同版本必须通过 refresh 创建新 inode 并原子替换 dentry。

对象版本的规范身份为：非空 VersionId 优先；没有 VersionId 时必须存在可用于 `If-Match` 的强 ETag，并在持久化前去除 HTTP 引号、规范化表示；弱 ETag 或空 ETag 拒绝导入。同一版本的判断只比较规范身份，不把 lastModified 当版本标识。导入时使用 checked arithmetic 验证 size、block count 和 range；需要超过 `uint32_t` block index 空间的对象拒绝导入。

BatchImport 每次最多 1000 项，返回逐项结果，不保证跨项原子性。单项以规范 path 和对象版本保持幂等；同一请求出现重复 path 时，这些重复项返回 InvalidArgument。调用方可以只重试失败项。

refresh 在替换 dentry 的同一 Metadata 事务中将旧 inode 标记为 `superseded/cacheAdmissionDisabled`。旧打开句柄继续引用旧 inode，且已有 READY block 仍可读取，但旧 inode 的 Enqueue、Acquire 和 Commit 必须拒绝；正在执行的旧 Loader 不能发布 READY。若 S3 支持旧 VersionId，旧句柄可以继续回源；若仅有 ETag 且对象已被覆盖，旧句柄读取返回版本冲突，不允许静默读取新版本。

启用导入前，mgmtd feature gate 必须确认所有活跃 Metadata writer 都通过节点租约上报所需 cache schema version；Import/Refresh 在 gate 未开启时返回 FeatureDisabled。Client/FUSE/Admin 请求携带 cache protocol version，Metadata 对不兼容版本返回 UpgradeRequired；即使遗漏协商，未知 `OriginFile` variant 的反序列化也必须失败。激活后禁止回滚到不识别该 variant 的版本。兼容性测试必须覆盖旧客户端读取新 inode 失败，以及旧 Metadata writer 不能反序列化后重写并丢失 Origin 字段。

### 4.2 ObjectStore Adapter

ObjectStore 隔离具体 S3 SDK，第一阶段只提供：

```cpp
headObject()
readRange()
```

不提供 LIST、PUT、DELETE 和 Multipart Upload。`readRange()` 必须携带规范版本身份：有 VersionId 时请求该精确版本，否则使用 `If-Match: ETag`。统一错误至少包括 NotFound、AccessDenied、VersionMismatch、Throttled、Timeout、Unavailable 和 InvalidResponse。

Range 参数使用 checked arithmetic；offset+length 溢出直接拒绝。零长度读取不发 HTTP 请求。非零合法 Range 必须返回匹配的 HTTP 206、精确 Content-Range 和精确 body length；意外的 200、短 body、超长 body 或范围不匹配均返回 InvalidResponse，不能把部分内容交给用户。

只对 timeout、429 和可恢复 5xx 做有预算的退避重试。NotFound、权限错误和版本冲突不自动重试。SDK 调用在独立 IO 线程池运行，不占用 RDMA、Metadata 或 Storage worker。

### 4.3 Metadata Cache Store

Metadata 保存 block 的逻辑状态、提供原子状态转换并生成 Read Plan，不负责搬运数据。第一阶段状态机为：

```text
NONE → QUEUED → LOADING → READY → CLEANING → NONE
          ↑         │
          └─ FAILED ←
```

`CLEANING` 服务于显式 Admin 清理和 READY 异常后的安全删除，不做策略化 victim 选择；`INVALID`、`EVICTING` 为后续阶段预留。关键不变量：

- NONE 不创建 FDB record。
- 同一 block 同时只有一个有效 Loader。
- 每次 Acquire 递增 `loadEpoch`。
- Commit 必须匹配 inode、loaderId、epoch、LOADING 状态和 inode 未 superseded 条件。
- READY 只在 Storage 写入成功并获得 checksum 后产生。
- READY identity 由 `loadEpoch + checksum + blockLength` 组成，并随 Read Plan 返回。
- ReportCacheBlockInvalid 必须携带观察到的 READY identity，使用 CAS 将匹配的 READY 转成 CLEANING；若已有新 READY，则 no-op。
- Cache Block Key 使用 inode 与 blockIndex，对象更新由新 inode 隔离。

容量由 Metadata 中的原子计数器权威管理：`used = READY/CLEANING bytes + reserved bytes`。NONE/FAILED 转 QUEUED 时，在同一 FDB 事务中按实际 blockLength 预留容量；若将超过 ceiling，拒绝该 block。预留贯穿 QUEUED 和 LOADING，并在 Commit 时从 reserved 原子转为 READY；terminal Fail 或完成 cleanup 时释放。READY 异常报告先 CAS 到 CLEANING 并继续计入 used，成功执行幂等 removeChunks 后才删除 record、释放容量。可重试的 FAILED 再次 Enqueue 时重新申请容量。批量准入允许逐 block 结果，但任一事务内不得超配。

最小 RPC 包括 GetFileReadPlan、EnqueueCacheBlocks、AcquireCacheBlocks、CommitCacheBlocks、FailCacheBlocks、ReportCacheBlockInvalid 和 AdminCleanupCacheBlocks。

### 4.4 GetFileReadPlan

一次 Read Plan 返回请求范围内各 block 的 block index、文件范围、origin range、cache state、ChunkId、ChainId、checksum、实际 block length、loadEpoch 和 READY identity。

offset 超过 EOF 时返回空读取；跨过 EOF 时截断。非 Origin 文件返回明确类型结果，使调用方转回原生读路径。单次请求硬限制最多 1000 个 block，更大的读取由 Client 分批规划；每一批都绑定同一 inode 和 Origin version，任一批返回不同身份时整次 read 重新规划。Read Plan 是快照，Client 必须容忍返回后状态变化。

### 4.5 CacheReadPipeline

CacheReadPipeline 获取 Read Plan，并发执行 cache hit 和 origin miss，再按文件偏移组装用户 buffer。内部边界建议为：

```text
ReadPlanner
CacheHitReader
OriginMissReader
BufferAssembler
EnsureCachedReporter
LocalMissSingleflight
```

READY block 使用 Storage `batchRead`；其他状态直接回源。为使 block checksum 契约明确，第一阶段命中时读取完整的实际 blockLength、验证指定 checksum 后再切片到用户范围。连续 miss block 合并为 Range GET，单个 Range 默认最大 256 MiB，超过后切分并受并发限制。READY block 返回 NotFound 或 checksum 错误时，仅失败 segment 回源，并携带观察到的 READY identity 异步上报；匹配的 READY 转为 CLEANING，由 Cache Manager 幂等删除后重新 Enqueue，延迟报告不得覆盖更新的 READY。

singleflight key 使用 origin、bucket、key、version 和对齐 range，只合并同一 Client 进程内完全相同的请求。Future 完成后立即移除，不作为长期数据缓存。

### 4.6 Cache Manager

Cache Manager 第一阶段只负责 Hint、去重、容量准入、调度和加载，不参与 hit。最小外部 API 为：

```cpp
EnsureCached()
GetCacheStatus()
```

内部包含 HintCoalescer、CapacityGate、LoaderScheduler 和 CacheLoader。所有 foreground miss 默认准入；相同 inode/block hint 在进程内合并；READY 和有效 LOADING 不重复建任务。EnsureCached 遇到已持久化的 QUEUED 时，必须幂等地将其附着或重新附着到当前进程 Scheduler，不能仅返回“已排队”。容量预留失败时返回 BYPASSED。调度器限制全局并发、单 Origin 并发和 inflight bytes。

Cache Manager 重启后不主动扫描恢复全部进程内队列；新的 miss 会重新附着 QUEUED。Acquire 可以原子回收 lease 已过期的 LOADING，分配新 loaderId 和更高 epoch。这样 QUEUED-before-acquire 与 expired-LOADING 均不会被永久遗留。

### 4.7 Storage 复用边界

Loader 使用现有 `batchWrite`，Client 使用现有 `batchRead`，继续复用现有 ChunkId、ChainId、路由和错误处理。缓存数据使用独立 `CACHE_DATA` Chain Table，并在 M0 增加 role、logicalCapacity 和查询/校验；OriginFile 只能引用 CACHE_DATA table，普通 File 不能引用它。

每次 cache block 写必须从 offset 0 写入恰好 actualBlockLength，并保证替换/截断同 ChunkId 的旧内容，尤其是较短的最后一个 block。实现前先验证现有 Storage op 是否具备该语义；若不具备，增加最小 full-chunk replace adapter。checksum 使用现有 `ChecksumInfo` 表示，算法由 CACHE_DATA table 配置并在 table version 内保持不变，覆盖完整 actualBlockLength。Loader 必须验证写后 length 和该 checksum，之后才能 Commit。模糊超时后的重试必须是幂等 full replacement，不能留下旧 tail。

第一阶段不修改 ChunkEngine 物理 key，不实现 Storage Event、emergency eviction 和 inventory scan。

显式清理和异常 READY 修复使用 fenced sequence：READY/FAILED 在 Metadata 中转为 CLEANING 并生成 cleanupEpoch，使 Client 回源；调用现有 `removeChunks`；成功后按 epoch 删除 record 并释放 READY/reserved bytes。NotFound 视为删除成功；部分失败的 block 保持 CLEANING。EnsureCached 遇到 CLEANING 时幂等重新附着 cleanup task，删除完成后再重新申请容量和排队。不得扩展完整删除事件链路。

### 4.8 FUSE 与 Native API

两种入口共用：

```cpp
CacheReadPipeline::read(inode, offset, output)
```

FUSE 只负责保存打开时的 inode 快照、按 inode variant 分流、转换错误码并完成 reply。普通 3FS 文件不调用 GetFileReadPlan，继续使用原路径。

只读防线从 M1 生效：OriginFile 的 O_WRONLY、O_RDWR、O_TRUNC、write/pwrite、truncate/ftruncate、fallocate/hole-punch 和可写 mmap（若支持）统一返回 native ReadOnlyFileSystem，并映射为 `EROFS`。不能等到 FUSE 接入后才阻止现有写路径。

### 4.9 鉴权、配置与凭证

inode 和 RPC 只保存 OriginId、bucket、key 与规范版本身份，不保存 endpoint、access key、secret、session token 或 SDK 对象。各进程通过本地受保护配置和 CredentialProvider 将 OriginId 解析为 endpoint、region、TLS 与凭证；日志不得输出凭证和签名 URL。

GetFileReadPlan 必须携带 UserInfo 和有效的 read/open session，并复用 inode read permission；不能仅凭 inodeId 绕过 namespace 权限。Import、Refresh、Cleanup 和全局 Status 要求明确的 cache-admin 权限。Cache Manager 使用独立 service identity，只能调用所需的 cache state RPC 和目标 CACHE_DATA chain。M0 必须固定完整 wire structs、请求上限、逐项错误和鉴权失败语义。

### 4.10 第一阶段 wire contract 最小字段

- ImportOriginFile：ReqBase/UserInfo、PathAt、OriginId、bucket、key、规范 version identity、objectSize、CACHE_DATA tableId、blockSize、stripeSize 和 permission；响应返回 inode 与 `CREATED/ALREADY_EXISTS`。
- BatchImportOriginFiles：最多 1000 个上述 entry；响应与输入等长，每项独立 Result，不做跨项回滚。
- RefreshOriginFile：cache-admin、PathAt、expectedInode、旧规范 version identity 和新 Origin metadata；响应返回新 inode，expectedInode 不匹配返回 Conflict。
- GetFileReadPlan：UserInfo、openSessionId、inode、offset、length、cacheProtocolVersion；响应绑定 inode 与规范 version identity，并返回最多 1000 个 ReadBlockPlan。
- Enqueue/Acquire/Commit/Fail：service identity、bounded block list；Acquire/Commit 携带 loaderId/loadEpoch，Commit 还携带 blockLength/checksum，逐项返回状态。
- ReportCacheBlockInvalid：UserInfo 或 service identity、inode/block、观察到的 READY identity、NotFound/ChecksumMismatch reason；只对匹配 READY 做 CAS。
- EnsureCached：service-authenticated client identity、inode、beginBlock、blockCount、reason 和 priority；它是短超时提示，响应只表示接收/旁路状态，不表示加载完成。
- AdminCleanupCacheBlocks：cache-admin、inode/range、可选 expected READY identity；响应逐 block 返回 CLEANED/RETRYING/NOT_FOUND/CONFLICT。

所有 bounded block list 第一阶段硬限制 1000 项；超限返回 RequestTooLarge。所有逐项接口保持输入顺序，并使用稳定错误码支持调用方只重试失败项。

## 5. 关键数据流

### 5.1 导入

```text
Admin CLI → ObjectStore HEAD → Metadata ImportOriginFile
          → 创建 Origin inode → 分配 CACHE_DATA Layout → 创建 dentry
```

### 5.2 Cold Read

```text
FUSE/Native → GetFileReadPlan → NONE
            → S3 Range GET → 用户 buffer
            → 异步 EnsureCached → 返回用户
```

用户 read 不等待 EnsureCached。Cache Manager 不可用不影响成功的 S3 回源。

### 5.3 后台加载

```text
EnsureCached → Hint 合并 → CapacityGate → Enqueue → Scheduler
             → Acquire → S3 GET → 拆分 block/checksum
             → Storage batchWrite → Commit READY
```

部分 Storage write 失败时，成功 block 可以分别 Commit READY，失败 block 标记 FAILED 并释放其 reservation。对象版本冲突或 inode 已 superseded 时禁止写入和 Commit。

Metadata Commit 超时后，Loader查询 record 判断是否已经 READY；状态仍不明确时记录指标，可能产生的 orphan 留给后续 Reconcile 处理。

### 5.4 Warm 与 Mixed Read

READY block 合并为 Storage batch，相邻 miss 合并为 Origin Range；两类读取可以并行。BufferAssembler 按文件偏移写入，不依赖完成顺序。首尾非对齐 block 只复制用户请求覆盖区间。

READY chunk NotFound 或 checksum mismatch 时，该 segment 回源；回源成功则用户读取成功，同时清除本地 Read Plan、携带 READY identity 做 CAS invalidation，并重新触发加载。任一必要 segment 最终失败时，整个 read 返回 I/O 错误，不返回静默损坏或不完整数据。

### 5.5 对象刷新

```text
cache-refresh-origin → S3 HEAD → 比较版本
                     → 创建新 inode → 原子替换 dentry
```

新 open 使用新 inode；已有 fd 保持旧 inode；新 inode 的 ChunkId 不会命中旧缓存。旧 inode 被原子标记为 superseded，已有 READY 可读但不得新 Enqueue/Acquire/Commit。第一阶段不立即清理旧 inode 对应 chunk。

### 5.6 容量上限

```text
Client miss → S3 正常返回
EnsureCached → CapacityGate → BYPASSED
```

容量满不会使用户读取失败。ceiling 按 Metadata 原子维护的 READY/CLEANING bytes + reserved bytes 判定，并发请求不能超配。Admin 可查看 READY、CLEANING、reserved 和 bypass 原因，并通过 CLEANING fenced flow 显式清理指定 inode 的缓存；不实现自动 victim 选择。

## 6. 错误与降级

| 故障 | 用户读取 | 缓存处理 |
|---|---|---|
| Cache Manager 不可用 | 回源成功则成功 | 不填充 |
| Metadata Read Plan 失败 | 失败 | 不变 |
| S3 超时或无权限 | 失败 | Loader 标记失败 |
| 对象版本冲突 | 返回 stale/I/O 错误 | 禁止 Commit |
| READY chunk 不存在 | 回源成功则成功 | 上报无效并重载 |
| EnsureCached 超时 | 不受影响 | 后台结果未知 |
| Cache 写入部分失败 | 不影响当前 foreground read | 成功块 READY，失败块 FAILED |
| 容量已满 | 回源成功则成功 | BYPASSED |
| 延迟 invalid 报告命中新 READY | 不受影响 | READY identity 不匹配，no-op |
| Manager 在 QUEUED 后重启 | 回源成功则成功 | 新 hint 重新附着 Scheduler |

第一阶段保留两项最小恢复能力：LOADING lease 超时回收，以及 READY 异常后的 ReportCacheBlockInvalid。它们用于避免状态永久卡死，不扩展为完整 Reconcile。

## 7. 纵向里程碑

### M0：接口契约与测试骨架

- 定义 Origin、Cache Block、Read Plan、EnsureCached 和 ObjectStore 协议。
- 使用独立 OriginFile variant，并定义 fail-closed feature gate 与升级/回滚规则。
- 固定 UserInfo/session、service identity、cache-admin 权限和 OriginId 凭证解析边界。
- 扩展并校验 CACHE_DATA Chain Table role、logicalCapacity 和容量计数结构。
- 预留 RPC method ID。
- 建立 mock、测试目录和非阻塞 benchmark 骨架。
- 默认 cache block 16 MiB，默认 Origin Range 最大 256 MiB；参数均可配置。

交付门槛：协议和 mock 可编译，原有协议测试不回归，核心接口不暴露 AWS SDK 或凭证类型，旧二进制遇到 OriginFile fail-closed。

### M1：Origin 文件导入和直接回源

- 实现单文件和批量导入、refresh 和 inode 版本替换。
- 实现 MinIO/AWS S3 HEAD、Range GET 和版本约束。
- 建立共享 CacheReadPipeline skeleton，Native API 使用其 miss-only reader；后续里程碑只向同一 pipeline 增加 Read Plan 和 hit。
- 从创建/open 开始启用全部 OriginFile 只读防线。

交付门槛：不同大小、非对齐、跨 block 和 EOF 读取内容正确，对象覆盖不会造成新旧混读，所有写入口 fail-closed，普通文件测试不回归。

### M2：Metadata 状态与 Read Plan

- 实现 Cache Block Store 和状态转换 RPC。
- 实现 loadEpoch/READY identity fencing、lease 回收、容量预留和 GetFileReadPlan。
- 批量读取 100 至 1000 个 block，不产生逐 block RPC 或 NONE 写入。

交付门槛：并发 Acquire 只有一个成功，旧 epoch 不能 Commit，延迟 invalidation 不能清除新 READY，并发准入不超过 ceiling，ChunkId/ChainId 与现有规则一致。

### M3：后台填充与二次命中

- 实现 Cache Manager、Hint 合并、容量门禁、Scheduler 和 Loader。
- 实现全 hit、全 miss、mixed read、Range 合并和 singleflight。
- 实现 READY NotFound/checksum mismatch 的回源降级和状态修复。
- 实现 retry-safe full-block replacement、checksum 校验、QUEUED 重附着和 expired LOADING 回收。

交付门槛：cold read、后台填充、warm hit 可重复验证；Client 不写 Storage；Cache Manager 不参与 hit；Cache Manager 停止时 cold read 仍可用。

### M4：FUSE 与完整用户入口

- 将 FUSE 接到 M1 已建立的共享 CacheReadPipeline，不创建第二套 Origin reader。
- 支持 open、stat、read、pread、list、readdir 和 permission 语义。
- 验证所有 FUSE 写入口映射为 EROFS；核心只读防线已在 M1 生效。
- 普通文件继续使用原生路径。

交付门槛：FUSE 与 Native API 结果一致，Cache Manager 不可用不阻塞 hint，refresh 前后的新旧 fd 行为正确。

### M5：兼容性与交付收口

- MinIO 自动化集成测试，以及使用外部凭证显式运行的 AWS S3 release-qualification 测试。
- 完成错误注入、基础指标、Admin CLI 和非阻塞 benchmark。
- 提供 `cache-import`、`cache-refresh-origin`、`cache-status`、`cache-list-blocks`，以及显式清理 inode cache 的管理能力。

交付门槛：第一阶段 Definition of Done 全部满足。性能数据只记录，不作为阻塞条件。

## 8. 测试计划

### 8.1 Metadata

- OriginFile serde、未知 variant fail-closed、导入幂等、BatchImport 逐项结果/上限/重复 path 和现有 path 冲突。
- 旧 Client 读取 OriginFile fail-closed，旧 Metadata writer 不能重写并丢字段，feature gate 阻止不兼容导入/回滚。
- Refresh 创建新 inode，expectedInode 防止并发覆盖。
- NONE/QUEUED/LOADING/READY/FAILED 状态转换。
- 重复 Enqueue、并发 Acquire、loaderId 和 epoch 冲突。
- lease 到期重获、inode 替换后旧 Loader Commit 失败。
- QUEUED-before-acquire 重启后由新 hint 重新附着。
- 延迟 ReportCacheBlockInvalid 与新 READY 竞态时 no-op。
- 并发容量预留不超配，Fail/Invalid/Cleanup 后正确释放。
- Admin cleanup 的 NotFound、重复请求和部分失败重试。
- Read Plan 的 EOF、非对齐和 0/1/100/1000 blocks。
- GetFileReadPlan session/权限校验，以及 Import/Refresh/Cleanup 的 cache-admin 校验。
- CACHE_DATA role 校验：OriginFile 拒绝 USER_DATA table，普通 File 拒绝 CACHE_DATA table。

### 8.2 ObjectStore

- MinIO HEAD、Range GET、VersionId/ETag，以及固定的 0、1、block-1、block、block+1 和跨多 block 对象。
- timeout、429、5xx 的预算重试。
- NotFound、AccessDenied、VersionMismatch 不重试。
- 200-to-Range、短 body、错误 Content-Range、range overflow、弱/空 ETag 和超大 block count 被拒绝。
- AWS S3 release-qualification 通过外部凭证显式启用；普通 CI 无凭证时以明确的 SKIPPED 状态结束，不冒充通过。

### 8.3 Client

- 全 miss、全 hit、mixed read 和 BufferAssembler。
- 相邻 Range 合并、256 MiB 切分和并发上限。
- singleflight、READY NotFound 和 checksum mismatch。
- 完整 block checksum 后切片，partial read corruption 可被发现。
- EnsureCached 超时不影响成功的 foreground read。

### 8.4 Cache Manager

- Hint 合并和重复任务抑制。
- 容量满 BYPASSED。
- Loader Range 合并、block 拆分和部分写失败。
- 模糊写超时重试和短 last-block 覆盖不会遗留旧 tail。
- VersionMismatch 禁止 Commit。
- 进程退出后 QUEUED 可重附着、expired LOADING 可重新加载。

### 8.5 FUSE 与回归

- open/stat/read/pread/readdir、并发和随机 range。
- O_WRONLY/O_RDWR/O_TRUNC、write/pwrite、truncate/ftruncate、fallocate/hole-punch 和可写 mmap 的只读错误语义。
- Native API 与 FUSE 结果一致。
- Origin 与普通文件共存，普通文件仍走原路径。
- 原有 Metadata、Storage、Client 和 FUSE 相关测试不回归。

### 8.6 基础可观测性与 benchmark

保留 cache hit/miss bytes、Origin 请求数和延迟、Read Plan 延迟、Loader queue/inflight、状态转换、bypass reason 等基础指标。保留 cold read、cache miss、warm hit、mixed read 和原生 3FS 对照 benchmark，但只用于建立基线和发现明显问题。

不设置吞吐百分比、P99 或压力时长的硬性门槛。

## 9. Definition of Done

- MinIO 自动化集成测试通过。
- AWS S3 release-qualification 使用外部凭证通过并保留测试证据；普通 CI 可以明确 SKIPPED。
- 单文件和批量 Origin 导入可用。
- Native API 和 FUSE 只读访问可用。
- cold read、后台填充、warm hit 闭环可重复验证。
- 全 hit、全 miss和 mixed read 内容正确。
- 对象版本冲突不会产生错误缓存或新旧混读。
- READY chunk 丢失或损坏后可以回源降级并修正状态。
- Cache Manager 不可用不影响成功的 S3 回源和已有 hit。
- 容量满后停止准入但不影响 foreground read。
- 容量计数在并发准入、失败、失效和显式清理后保持一致。
- 基础 Admin CLI 和指标可用。
- benchmark 可运行并产出基线数据，但结果不阻塞交付。
- 原生 3FS 文件的行为和相关测试不回归。
- 后续阶段能力以明确 backlog 记录，不以隐含 TODO 留在关键路径。
