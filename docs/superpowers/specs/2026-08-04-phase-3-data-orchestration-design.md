# 3FS Cache 第三阶段数据编排设计

日期：2026-08-04

状态：实现基线

## 1. 目标

第三阶段在已完成的只读缓存闭环和物理容量闭环之上增加面向 GPU Job 的显式数据编排：

```text
用户提交数据源
  -> Planner 冻结 namespace/object 版本并生成 block plan
  -> Job Scheduler 按优先级和配额提交显式 admission
  -> 现有 Loader + Storage Permit 写入 Cache
  -> Job Tracker 计算 Ready Ratio
  -> READY 后保留或释放 owner-scoped Pin
```

缓存命中、Origin fallback、物理容量门禁、generation fence、coordinated retire 和 Storage Event 协议保持不变。

## 2. 范围

本阶段交付：

- 持久化 Prefetch Job，以及 create/get/list/cancel API；
- namespace path、path list、manifest 和 S3 prefix 四类 Planner；
- 可恢复、分页、去重的 block plan；
- 严格优先级、每 Job 并发限制和带宽限制；
- 基于逻辑 block bytes 的 Ready Ratio；
- 活动 Job 保护、READY 后 Pin TTL、显式 Pin/Unpin；
- CLI、指标、MinIO 集成和故障验收。

本阶段不交付：

- 多 Cache Manager、leader election 或跨 Manager 分布式调度；
- 通用 Metadata/Storage 双向 inventory reconcile；
- 写入缓存、multipart upload 或 Origin publish；
- 目录持续同步、prefix 删除传播或增量 watch；
- 性能硬指标和复杂公平调度。

## 3. 已确定约束

1. 仍只运行一个 Cache Manager。
2. Job、plan 和 pin 的权威状态持久化在 Metadata 使用的 FDB；Cache Manager 内存队列只是执行缓存。
3. 显式 Prefetch 跳过 second-miss 策略，但不能绕过 Phase-2 routing、Storage Permit、high watermark 和 fail-closed 检查。
4. 优先级保持现有语义：有符号非负整数，数值越大优先级越高；相同优先级按 FIFO。
5. Ready Ratio 使用 `requiredReadyBps`（1..10000）表示，避免浮点状态转换不一致。
6. Job READY 是单调终态；同时返回当前 ready bytes 供诊断。活动 Job 的 plan blocks 在终态前受 Job Pin 保护。
7. Pin 按 owner 持久化，重叠 Job/显式 Pin 互不覆盖；只有最后一个有效 owner 消失后 block 才可驱逐。
8. Cancel 不撤销正在执行的 Storage write。它必须阻止尚未开始的 plan block，并安全释放只属于该 Job 的排队 claim。
9. Manifest v1 是 UTF-8 文本，每个非空、非注释行是一个绝对 3FS namespace path。
10. Prefix Planner 使用 S3 ListObjectsV2 分页，按 object key 排序，并通过现有 ImportOriginFile 语义导入目标 namespace。

## 4. 持久化模型

### 4.1 Job identity 与状态

`PrefetchJobId` 使用非零 UUID。创建请求携带客户端生成的 job ID，因 RPC timeout 重试时保持相同 ID。

```cpp
enum class PrefetchJobState : uint8_t {
  PENDING,
  PLANNING,
  LOADING,
  PARTIAL_READY,
  READY,
  FAILED,
  CANCELLED,
};

struct PrefetchJobSpec {
  PrefetchJobId jobId;
  Uid ownerUid;
  vector<DatasetSource> sources;
  uint32_t priority;
  uint32_t maxParallelLoads;
  uint64_t bandwidthLimitBytesPerSec;
  uint32_t requiredReadyBps;
  bool pinAfterReady;
  Duration pinTtl;
};
```

`PrefetchJobRecord` 额外保存创建/更新时间、状态版本、planner cursor、错误码、planned/ready/failed bytes、block counters 和取消 epoch。所有状态更新使用 job ID + state version CAS。
认证使用请求中的 `UserInfo`，但持久化 Job 只保存 `ownerUid`，不得保存用户 token。

### 4.2 Dataset source

```cpp
enum class DatasetSourceType : uint8_t {
  NAMESPACE_PATH,
  PATH_LIST,
  MANIFEST_PATH,
  S3_PREFIX,
};
```

- `NAMESPACE_PATH`：文件或递归目录；
- `PATH_LIST`：有界的绝对路径列表；
- `MANIFEST_PATH`：指向一个 Origin-backed manifest 文件；
- `S3_PREFIX`：origin ID、bucket、prefix、目标 namespace root 和 import layout。

Wire 请求限制 source 数、path 数、字符串长度和序列化总量。Planner 不能接受相对路径、`..` 逃逸、空 bucket/prefix 或未知 Origin。

### 4.3 Plan entry

```cpp
struct PrefetchPlanEntry {
  PrefetchJobId jobId;
  CacheBlockKey key;
  uint64_t blockLength;
  uint32_t priority;
  PrefetchPlanEntryState state;
  optional<PermitIdentity> admissionPermit;
};
```

主键为 `(jobId, inode, block)`，因此重复 source、目录重叠和 manifest 重复行不会重复计数。每个 plan page 在一个事务内写入 entries 并更新 planned bytes/count。计划结束前 Job 不进入 LOADING。

### 4.4 Pin

Pin 使用双索引：

```text
(block key, owner kind, owner id) -> PinRecord
(owner kind, owner id, block key) -> PinRecord
```

owner kind 为 `ACTIVE_JOB` 或 `EXPLICIT_PIN`。记录包含创建时间、过期时间和 generation fence（READY 时填入）。活动 Job 使用可续租 lease；READY 且 `pinAfterReady=true` 时原子转换为固定 TTL。过期、取消或 unpin 删除该 owner 的全部索引，不影响其他 owner。

## 5. Planner

### 5.1 Namespace Planner

Planner 使用现有权限与 path lookup 语义。文件必须是未 supersede 的 OriginFile。目录递归使用稳定分页 cursor；每个文件解析时冻结 inode、object version 和 layout。路径随后被 refresh 到新 inode 不会改变已经生成的 plan。

每个文件按真实长度生成 block：

```text
blockLength = min(chunkSize, fileLength - blockOffset)
```

空文件不生成 block。整个 Job 没有 block 时以 `EMPTY_PLAN` 失败，不能伪装成 READY。

### 5.2 Manifest Planner

Manifest v1：

- UTF-8，一行一个绝对 namespace path；
- 空行和以 `#` 开头的行忽略；
- 禁止 NUL、相对路径和超长行；
- 配置限制 manifest bytes、行数和展开后的 blocks；
- manifest Origin version 在首次读取时冻结，Range GET 始终携带 VersionId/If-Match。

解析得到的路径进入同一个 Namespace Planner，保持去重和权限语义。

### 5.3 Prefix Planner

ObjectStore 增加有界 `listObjects`：输入 bucket/prefix/continuation/maxKeys，输出按 key 排序的 object metadata 和 next token。S3 实现使用 ListObjectsV2，拒绝无进展 token、重复页和超过限制的响应。

每个 object key 映射为：

```text
destinationRoot / relativeKey
```

映射必须保持在 destination root 内。Planner 使用现有 ImportOriginFile 幂等导入；object version/etag 变化遵循新 inode 原子替换规则。Prefix 是一次性快照，不删除 namespace 中已经存在但本次未列出的文件。

### 5.4 恢复

每种 source 的 cursor 均持久化。Cache Manager 重启后重新加载 PENDING/PLANNING Job，从最后提交页继续；重复页由 plan 主键去重。Planner 失败记录稳定错误，不能留下可执行的半计划 Job。

## 6. Admission 与调度

### 6.1 显式 admission

AdmissionContext 增加 reason、priority、job ID 和 explicit 标志。`PREFETCH` 直接返回 ADMIT，但后续仍执行：

```text
routing/rollout gate
  -> physical preflight
  -> durable Storage permits
  -> Metadata enqueue
  -> Loader Scheduler
```

容量不足时 Job 保持 PARTIAL_READY/LOADING 并可重试；不能把失败伪装成 READY，也不能挤掉 pin block 以外的安全约束。

### 6.2 Job Scheduler

调度 key 为：

```text
(-priority, enqueueSequence, jobId, inode, block)
```

实现中沿用“较大 priority 先出队”，同优先级 FIFO。一个 Job 的 inflight 不超过 `maxParallelLoads`。全局和 per-Origin CapacityGate 继续生效。带宽限制使用可测试的 token bucket，只限制后台调度，不阻塞 Client foreground fallback。

连续 Range batching 只有在 inode、priority、owner/配额上下文兼容时发生，不能让低优先级 Job 借相邻 block 越级。

### 6.3 共享 block

不同 Job 或 foreground miss 可指向同一 block。Metadata cache state 仍是全局去重权威。每个 Job 单独保存 plan entry：

- 已 READY：立即计入该 Job；
- 已 QUEUED/LOADING：Job attach 到共享完成通知；
- NONE/FAILED：当前调度 Job 可创建 admission；
- 一个 Job cancel 不删除其他 owner 的 hint、permit 或 pin。

## 7. Ready Ratio 与状态机

```text
readyRatioBps = floor(readyBytes * 10000 / plannedBytes)
```

乘法和除法使用溢出安全整数运算。只有 generation-fenced READY 才计入 ready bytes。Job 状态：

```text
PENDING -> PLANNING -> LOADING
LOADING -> PARTIAL_READY -> READY
非终态 -> CANCELLED
非终态 -> FAILED
```

当 `readyBytes * 10000 >= plannedBytes * requiredReadyBps` 时，事务性进入 READY。READY 不因后续 eviction 回退；未设置 `pinAfterReady` 的 Job 状态表示“曾达到启动门槛”，status 同时显示当前可用比例。

## 8. Cancel 与 Pin

Cancel 先持久化 `CANCELLED` 和 cancel epoch，再停止 Planner/调度：

1. 未 admission 的 plan entry 不再提交；
2. 只属于该 Job 且尚未被 Loader acquire 的 QUEUED claim 使用 permit + attempt fence 取消；
3. 共享 QUEUED/LOADING 和已经开始的 write 继续收敛；
4. 删除 ACTIVE_JOB pins；
5. 重复 cancel 返回同一终态。

显式 Pin 使用与 Prefetch 相同 Planner。Pin 请求本身不假设数据已 READY；可选择 `prefetchMissing=true`。Unpin 只删除指定 owner。Eviction candidate 查询必须在 BeginEvict 前确认没有有效 pin；竞争由 Metadata 事务 fence 关闭。

## 9. API、CLI 与可观测性

Cache Manager 追加 wire method，不能改现有 method ID：

- `createPrefetchJob`
- `getPrefetchJob`
- `listPrefetchJobs`
- `cancelPrefetchJob`
- `pinDataset`
- `unpinDataset`
- `getPinStatus`

CLI：

```text
cache-prefetch create|status|list|cancel
cache-pin create|status|remove
```

指标至少包括 job state transition、planner pages/objects/blocks、scheduler queue/inflight、ready ratio、cancelled blocks、pin owners/bytes、quota wait 和失败 reason。日志携带 job ID、source index、inode/block、permit/placement identity，但不得记录凭证或完整 manifest 内容。

## 10. 故障与安全不变量

1. RPC 重试不能创建第二个 Job。
2. 重复 planner page 不能重复 planned bytes。
3. 显式 Prefetch 永远不能绕过物理容量权威。
4. Job cancel 不能取消共享或已执行 write。
5. 一个 pin owner 的删除不能解除其他 owner 的保护。
6. 有效 pin 与 BeginEvict 的竞争由同一 Metadata 事务决定。
7. READY 只统计 Metadata generation-fenced READY，不以队列完成回调代替。
8. Planner/Manager 崩溃不能留下无 Job 归属、无法恢复的 permit。
9. Prefix/manifest 展开必须有硬上限并可分页，不能把不可信输入一次性载入内存。
10. 所有编排失败不影响 Client 命中和 Origin fallback。

## 11. 升级

Phase 3 schema/protocol 只追加字段、枚举值和 service method。默认 `enable_phase3=false`。启用前要求 Phase 2 已 ENABLED、所有 Cache Manager/Metadata/Client CLI 具备 Phase-3 capability，且不存在旧 Manager。禁用时先停止新 Job，取消或完成非终态 Job，释放 ACTIVE_JOB pins，再回退二进制。
