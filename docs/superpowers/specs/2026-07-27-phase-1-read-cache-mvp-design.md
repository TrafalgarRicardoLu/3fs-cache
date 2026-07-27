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

- Origin-backed File Schema 和 namespace 导入、刷新。
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

## 4. 组件与职责

### 4.1 Origin Namespace

Origin Namespace 将显式指定的对象映射成普通 3FS 文件，并保存对象身份与版本。第一阶段提供：

```cpp
ImportOriginFile(path, origin, layout)
BatchImportOriginFiles(files)
RefreshOriginFile(path, expectedInode, newOrigin)
```

导入前通过 ObjectStore HEAD 获取对象 size、ETag 和可选 VersionId。导入操作只创建 namespace，不主动下载对象。path 已存在普通文件时默认拒绝覆盖；已存在同版本 Origin 文件时幂等返回；不同版本必须通过 refresh 创建新 inode 并原子替换 dentry。

旧打开句柄继续引用旧 inode。若 S3 支持旧 VersionId，旧句柄可以继续回源；若仅有 ETag 且对象已被覆盖，旧句柄读取返回版本冲突，不允许静默读取新版本。

### 4.2 ObjectStore Adapter

ObjectStore 隔离具体 S3 SDK，第一阶段只提供：

```cpp
headObject()
readRange()
```

不提供 LIST、PUT、DELETE 和 Multipart Upload。`readRange()` 必须携带 VersionId；没有 VersionId 时使用 `If-Match: ETag`。统一错误至少包括 NotFound、AccessDenied、VersionMismatch、Throttled、Timeout、Unavailable 和 InvalidResponse。

只对 timeout、429 和可恢复 5xx 做有预算的退避重试。NotFound、权限错误和版本冲突不自动重试。SDK 调用在独立 IO 线程池运行，不占用 RDMA、Metadata 或 Storage worker。

### 4.3 Metadata Cache Store

Metadata 保存 block 的逻辑状态、提供原子状态转换并生成 Read Plan，不负责搬运数据。第一阶段状态机为：

```text
NONE → QUEUED → LOADING → READY
          ↑         │
          └─ FAILED ←
```

`INVALID` 和 `EVICTING` 可以在协议中预留，但不实现完整淘汰流程。关键不变量：

- NONE 不创建 FDB record。
- 同一 block 同时只有一个有效 Loader。
- 每次 Acquire 递增 `loadEpoch`。
- Commit 必须匹配 inode、loaderId、epoch 和 LOADING 状态。
- READY 只在 Storage 写入成功并获得 checksum 后产生。
- Cache Block Key 使用 inode 与 blockIndex，对象更新由新 inode 隔离。

最小 RPC 包括 GetFileReadPlan、EnqueueCacheBlocks、AcquireCacheBlocks、CommitCacheBlocks、FailCacheBlocks 和 ReportCacheBlockInvalid。

### 4.4 GetFileReadPlan

一次 Read Plan 返回请求范围内各 block 的 block index、文件范围、origin range、cache state、ChunkId、ChainId、checksum 和实际 block length。

offset 超过 EOF 时返回空读取；跨过 EOF 时截断。非 Origin 文件返回明确类型结果，使调用方转回原生读路径。单次请求建议限制最多 1000 个 block，更大的读取由 Client 分批规划。Read Plan 是快照，Client 必须容忍返回后状态变化。

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

READY block 使用 Storage `batchRead`；其他状态直接回源。连续 miss block 合并为 Range GET，单个 Range 默认最大 256 MiB，超过后切分并受并发限制。READY block 返回 NotFound 或 checksum 错误时，仅失败 segment 回源，并异步上报状态无效和重新发送 EnsureCached。

singleflight key 使用 origin、bucket、key、version 和对齐 range，只合并同一 Client 进程内完全相同的请求。Future 完成后立即移除，不作为长期数据缓存。

### 4.6 Cache Manager

Cache Manager 第一阶段只负责 Hint、去重、容量准入、调度和加载，不参与 hit。最小外部 API 为：

```cpp
EnsureCached()
GetCacheStatus()
```

内部包含 HintCoalescer、CapacityGate、LoaderScheduler 和 CacheLoader。所有 foreground miss 默认准入；相同 inode/block hint 合并；READY、LOADING 和 QUEUED 不重复建任务。达到逻辑容量上限后返回 BYPASSED。调度器限制全局并发、单 Origin 并发和 inflight bytes。

Cache Manager 重启后不恢复进程内队列，但 LOADING lease 到期后，新的 miss 可以重新触发加载，避免 block 永久卡死。

### 4.7 Storage 复用边界

Loader 使用现有 `batchWrite`，Client 使用现有 `batchRead`，继续复用现有 ChunkId、ChainId、路由、checksum 和错误处理。缓存数据使用独立 `CACHE_DATA` Chain Table。第一阶段不修改 ChunkEngine 物理 key，不实现 Storage Event、emergency eviction 和 inventory scan。

只有当测试和显式管理清理无法使用现有 `removeChunks` 时，才增加最小适配；不得在第一阶段扩展完整删除事件链路。

### 4.8 FUSE 与 Native API

两种入口共用：

```cpp
CacheReadPipeline::read(inode, offset, output)
```

FUSE 只负责保存打开时的 inode 快照、按 backing type 分流、转换错误码并完成 reply。普通 3FS 文件不调用 GetFileReadPlan，继续使用原路径。

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

部分 Storage write 失败时，成功 block 可以分别 Commit READY，失败 block 标记 FAILED。对象版本冲突时禁止写入和 Commit。

Metadata Commit 超时后，Loader查询 record 判断是否已经 READY；状态仍不明确时记录指标，可能产生的 orphan 留给后续 Reconcile 处理。

### 5.4 Warm 与 Mixed Read

READY block 合并为 Storage batch，相邻 miss 合并为 Origin Range；两类读取可以并行。BufferAssembler 按文件偏移写入，不依赖完成顺序。首尾非对齐 block 只复制用户请求覆盖区间。

READY chunk NotFound 或 checksum mismatch 时，该 segment 回源；回源成功则用户读取成功，同时清除本地 Read Plan、上报 block invalid 并重新触发加载。任一必要 segment 最终失败时，整个 read 返回 I/O 错误，不返回静默损坏或不完整数据。

### 5.5 对象刷新

```text
cache-refresh-origin → S3 HEAD → 比较版本
                     → 创建新 inode → 原子替换 dentry
```

新 open 使用新 inode；已有 fd 保持旧 inode；新 inode 的 ChunkId 不会命中旧缓存。第一阶段不立即清理旧 inode 对应 chunk。

### 5.6 容量上限

```text
Client miss → S3 正常返回
EnsureCached → CapacityGate → BYPASSED
```

容量满不会使用户读取失败。Admin 可查看使用量和 bypass 原因，并可显式清理指定 inode 的缓存；不实现自动 victim 选择。

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

第一阶段保留两项最小恢复能力：LOADING lease 超时回收，以及 READY 异常后的 ReportCacheBlockInvalid。它们用于避免状态永久卡死，不扩展为完整 Reconcile。

## 7. 纵向里程碑

### M0：接口契约与测试骨架

- 定义 Origin、Cache Block、Read Plan、EnsureCached 和 ObjectStore 协议。
- 预留 RPC method ID。
- 建立 mock、测试目录和非阻塞 benchmark 骨架。
- 默认 cache block 16 MiB，默认 Origin Range 最大 256 MiB；参数均可配置。

交付门槛：协议和 mock 可编译，原有协议测试不回归，核心接口不暴露 AWS SDK 类型。

### M1：Origin 文件导入和直接回源

- 实现单文件和批量导入、refresh 和 inode 版本替换。
- 实现 MinIO/AWS S3 HEAD、Range GET 和版本约束。
- Native API 可读取 Origin 文件；此时所有 block 均直接回源。

交付门槛：不同大小、非对齐、跨 block 和 EOF 读取内容正确，对象覆盖不会造成新旧混读，普通文件测试不回归。

### M2：Metadata 状态与 Read Plan

- 实现 Cache Block Store 和状态转换 RPC。
- 实现 loadEpoch fencing、lease 回收和 GetFileReadPlan。
- 批量读取 100 至 1000 个 block，不产生逐 block RPC 或 NONE 写入。

交付门槛：并发 Acquire 只有一个成功，旧 epoch 不能 Commit，ChunkId/ChainId 与现有规则一致。

### M3：后台填充与二次命中

- 实现 Cache Manager、Hint 合并、容量门禁、Scheduler 和 Loader。
- 实现全 hit、全 miss、mixed read、Range 合并和 singleflight。
- 实现 READY NotFound/checksum mismatch 的回源降级和状态修复。

交付门槛：cold read、后台填充、warm hit 可重复验证；Client 不写 Storage；Cache Manager 不参与 hit；Cache Manager 停止时 cold read 仍可用。

### M4：FUSE 与完整用户入口

- FUSE 和 Native API 共用 CacheReadPipeline。
- 支持 open、stat、read、pread、list、readdir 和 permission 语义。
- Origin 文件写相关操作返回只读错误。
- 普通文件继续使用原生路径。

交付门槛：FUSE 与 Native API 结果一致，Cache Manager 不可用不阻塞 hint，refresh 前后的新旧 fd 行为正确。

### M5：兼容性与交付收口

- MinIO 自动化集成测试和 AWS S3 可选兼容测试。
- 完成错误注入、基础指标、Admin CLI 和非阻塞 benchmark。
- 提供 `cache-import`、`cache-refresh-origin`、`cache-status`、`cache-list-blocks`，以及显式清理 inode cache 的管理能力。

交付门槛：第一阶段 Definition of Done 全部满足。性能数据只记录，不作为阻塞条件。

## 8. 测试计划

### 8.1 Metadata

- Origin File 新旧数据兼容、导入幂等和 path 冲突。
- Refresh 创建新 inode，expectedInode 防止并发覆盖。
- NONE/QUEUED/LOADING/READY/FAILED 状态转换。
- 重复 Enqueue、并发 Acquire、loaderId 和 epoch 冲突。
- lease 到期重获、inode 替换后旧 Loader Commit 失败。
- Read Plan 的 EOF、非对齐和 0/1/100/1000 blocks。

### 8.2 ObjectStore

- MinIO HEAD、Range GET、VersionId/ETag 和各种对象大小。
- timeout、429、5xx 的预算重试。
- NotFound、AccessDenied、VersionMismatch 不重试。
- AWS S3 兼容测试通过外部凭证显式启用。

### 8.3 Client

- 全 miss、全 hit、mixed read 和 BufferAssembler。
- 相邻 Range 合并、256 MiB 切分和并发上限。
- singleflight、READY NotFound 和 checksum mismatch。
- EnsureCached 超时不影响成功的 foreground read。

### 8.4 Cache Manager

- Hint 合并和重复任务抑制。
- 容量满 BYPASSED。
- Loader Range 合并、block 拆分和部分写失败。
- VersionMismatch 禁止 Commit。
- 进程退出后 lease 到期可重新加载。

### 8.5 FUSE 与回归

- open/stat/read/pread/readdir、并发和随机 range。
- Origin 文件只读错误语义。
- Native API 与 FUSE 结果一致。
- Origin 与普通文件共存，普通文件仍走原路径。
- 原有 Metadata、Storage、Client 和 FUSE 相关测试不回归。

### 8.6 基础可观测性与 benchmark

保留 cache hit/miss bytes、Origin 请求数和延迟、Read Plan 延迟、Loader queue/inflight、状态转换、bypass reason 等基础指标。保留 cold read、cache miss、warm hit、mixed read 和原生 3FS 对照 benchmark，但只用于建立基线和发现明显问题。

不设置吞吐百分比、P99 或压力时长的硬性门槛。

## 9. Definition of Done

- MinIO 自动化集成测试通过。
- AWS S3 兼容测试通过。
- 单文件和批量 Origin 导入可用。
- Native API 和 FUSE 只读访问可用。
- cold read、后台填充、warm hit 闭环可重复验证。
- 全 hit、全 miss和 mixed read 内容正确。
- 对象版本冲突不会产生错误缓存或新旧混读。
- READY chunk 丢失或损坏后可以回源降级并修正状态。
- Cache Manager 不可用不影响成功的 S3 回源和已有 hit。
- 容量满后停止准入但不影响 foreground read。
- 基础 Admin CLI 和指标可用。
- benchmark 可运行并产出基线数据，但结果不阻塞交付。
- 原生 3FS 文件的行为和相关测试不回归。
- 后续阶段能力以明确 backlog 记录，不以隐含 TODO 留在关键路径。
