# 3FS Cache 第四阶段：可靠性恢复与写入设计

## 1. 目标与范围

第四阶段在前三阶段只读缓存、物理容量管理和数据编排之上完成两个闭环：

1. Cache Manager、Metadata 和 Storage 在进程退出、超时、丢块和跨版本残留后最终收敛；
2. 新文件先写入 3FS staging，可靠上传到 S3，再原子发布为 OriginFile。

本阶段不实现已有 OriginFile 的随机覆写、并发多写者合并、目录级事务、对象删除传播或通用对象网关。
第一版写入只支持新文件、单写者、顺序写；`fsync` 和可报告错误的 close/flush 在对象完成并发布后才成功。

Phase 4 默认关闭，启用它必须同时满足 Phase 2 和 Phase 3 capability。任何恢复或写入故障都不能破坏前台
Origin fallback，也不能把未完成对象发布到 namespace。

## 2. 已冻结的产品假设

- CACHE_ONLY target 只存放缓存数据，不与普通 3FS 用户数据混放；库存扫描仍校验 target role 和 cache
  descriptor，错误角色一律 fail closed。
- Cache 数据可丢弃，S3 是 OriginFile 的权威数据源；WRITE_STAGING 在上传完成前不是缓存，不能被正常或
  本地缓存淘汰策略处理。
- 能否继续 cache 只由 Storage 的物理容量和 permit 决定。Metadata logical bytes 只用于查询、状态核对和
  泄漏诊断，不参与 admission，也不用于计费。
- 正常驱逐继续通过可配置 `EvictionPolicy` 抽象，默认 LRU；Reconciler 只修复不一致，不替代容量驱逐策略。

## 3. 安全不变量

1. `(CacheBlockKey, cacheGeneration, placement)` 唯一标识一次物理缓存写入。
2. Loader 的 Commit、Fail 和恢复操作必须匹配 `loaderId + loadEpoch`；旧进程永远不能提交新 epoch。
3. READY 只有在持久化 placement 的每个 replica 都存在匹配 generation 时才有效。
4. Storage inventory 中没有对应 Metadata 所有权的 cache generation 是 orphan；删除必须携带 generation
   fence，不能删除扫描之后写入的新 generation。
5. Reconcile 只减少不确定性：无法证明安全时保留数据、记录状态并重试，不猜测成功。
6. staging 数据在对象 Complete 和 Metadata publish 之前不可作为 OriginFile 被打开。
7. publish 以 `uploadJobId + expectedStagingInode` 做幂等 CAS；同一 job 重试只能得到同一 OriginFile。
8. S3 凭证和 multipart 临时签名不进入 FDB、日志、RPC 或 job record。

## 4. 恢复架构

### 4.1 启动屏障

Cache Manager 启动顺序固定为：

```text
refresh routing
  -> recover permits and expired LOADING
  -> resume EVICTING/CLEANING
  -> recover orchestration and upload jobs
  -> complete one bounded reconcile bootstrap pass
  -> enable new admission and background workers
```

屏障期间 Cache hit 和 Origin fallback 可用，但新的 cache admission 返回可重试的 unavailable/bypass。启动扫描
使用稳定 cursor；每个 mutation 都幂等，因此进程再次退出后可以从头重放。

### 4.2 LOADING lease recovery

现有 Acquire 已能在 miss 到达时接管过期 lease，但启动恢复必须主动扫描全部 LOADING。Metadata 返回冻结的
key、loader fence、lease deadline、generation、block length、permit 和 placement。

- lease 未过期且 permit 为 PINNED：视为可能仍在执行，只登记下次检查时间；不抢占。
- lease 未过期但 permit/placement 已不存在：进入 fenced failure/cleanup。
- lease 已过期：以原 `loaderId + loadEpoch` 调用恢复型 Fail，递增 fence，进入 CLEANING；按记录的 generation
  清理所有 replica，完成后 REENQUEUE 或 FAILED。
- 物理数据完整但 Metadata 未 Commit：第一版仍清理后重载，不尝试从 Storage 反向合成 Commit，避免缺少
  原 Loader 校验上下文时发布错误 checksum/length。

恢复型 Fail 与普通 Fail 共用状态机，不增加旁路 mutation。LOADING permit 只在物理清理已接管后释放。

### 4.3 Metadata → Storage 检查

Metadata 按稳定 key 分页给出 READY、EVICTING、CLEANING 和 LOADING 记录。READY 检查其持久化 placement：

- 全部 replica generation/descriptor 匹配：保持 READY；
- 任一 replica 缺失、retired、长度或 checksum 不一致：进入现有 INVALID/CLEANING 流程；
- Storage 暂时不可达：不改变 Metadata，记录 retryable 结果。

EVICTING/CLEANING 重放其持久化 operation identity。LOADING 交给 lease recovery，两个 worker 不并发取得同一
record 的完成权。

### 4.4 Storage → Metadata inventory

新增只面向 CACHE_ONLY target 的分页 RPC。cursor 是 opaque 且只在同一 target snapshot attempt 内使用：

```cpp
struct CacheInventoryEntry {
  TargetId target;
  PhysicalDiskId disk;
  CacheBlockKey key;
  CacheGeneration generation;
  uint64_t payloadLength;
  ChecksumInfo checksum;
  bool retired;
};
```

Storage 从持久化 cache descriptor 生成条目，不从 ChunkId 猜测 generation。请求限制 target、cursor 和 limit；
响应携带 nextCursor、done 和 inventory epoch。epoch 改变时 Manager 丢弃本轮 cursor 并重扫。

Manager 批量查询 Metadata 后应用以下矩阵：

| Storage generation | Metadata 状态 | 动作 |
| --- | --- | --- |
| exact | READY/LOADING/EVICTING/CLEANING | 由对应正向状态机处理 |
| older | 任意含更新 generation 的记录 | generation-fenced retire |
| newer | 任意记录 | 不自动认领；标记 conflict，等待下一轮或管理员处理 |
| 任意 | 无记录、FAILED、NONE | generation-fenced retire orphan |

“无记录可删除”依赖 CACHE_ONLY 只含 cache 的冻结假设。删除仍走现有 retire/tombstone/event 协议；Reconciler
不能直接 unlink chunk，也不能直接减少 Metadata counters。

### 4.5 进度、背压与可观测性

Reconcile 按 node/target 限制并发和 page size，保存最近成功时间、扫描条目数、orphan 数、missing 数、conflict
数和错误分类。周期扫描不持久化 cursor；一次进程重启允许从头开始。Admin 可触发一轮、查询状态和 dry-run，
但 dry-run 只报告，不 mutation。

## 5. WRITE_STAGING 数据模型

### 5.1 Chain role

Chain table role 增加 `WRITE_STAGING`。它与 `CACHE_ONLY` 物理隔离：

- 普通 File 只有处于 write-through job 时可引用 WRITE_STAGING；
- OriginFile 只能引用 CACHE_DATA；
- normal/local cache eviction 和 Cache Inventory 必须排除 WRITE_STAGING；
- staging 使用普通 3FS replica write/read/checksum 能力和独立物理容量配置。

### 5.2 持久化 UploadJob

```cpp
enum class UploadJobState {
  OPEN, SEALED, UPLOADING, COMPLETING, PUBLISHING, PUBLISHED,
  ABORTING, FAILED, CANCELLED
};

struct UploadJobRecord {
  UploadJobId jobId;
  Uid ownerUid;
  Path path;
  InodeId stagingInode;
  uint64_t stagingLength;
  ObjectRef destination;
  UploadId multipartId;
  uint32_t nextPartNumber;
  vector<CompletedPart> parts;
  UploadJobState state;
  uint64_t stateVersion;
  Uuid writerLeaseId;
  uint64_t writerLeaseExpiresAtMs;
  optional<ImmutableObjectIdentity> completedObject;
  optional<InodeId> publishedInode;
};
```

parts 有数量和总 serialized bytes 上限。状态和 part checkpoint 以 `stateVersion` CAS。对象 key 由配置 root、
规范化 namespace path 和 jobId 确定；同一 job 重试不得换 key。

### 5.3 创建、seal 与 writer lease

专用 Metadata 操作原子创建 path dentry、普通 staging inode 和 UploadJob。第一版拒绝已存在 path、硬链接、
rename、第二个 writer、随机 offset、hole、truncate 和 append reopen。每次写续租 writer lease。

`fsync`/flush 先冻结最终 length 和 inode version，状态 OPEN→SEALED；seal 后所有写入失败。writer 进程退出且
lease 到期时，空 job 可取消，有数据 job 可由 Upload Worker seal 并恢复上传，策略由配置决定，默认恢复。

## 6. Multipart upload

ObjectStore 增加 vendor-neutral multipart 接口：create、uploadPart、complete、abort，以及校验 completed object
的 HEAD。S3 executor 负责 AWS 类型转换和错误映射。

- part size 可配置，除末 part 外满足 S3 最小值；最大 part 数和对象大小启动时校验。
- Worker 从 frozen staging inode 连续读取，每个 part 计算 checksum，再上传并持久化 ETag/checksum checkpoint。
- ambiguous UploadPart 以相同 uploadId/partNumber 重试；S3 的同 part number 覆盖语义使其幂等。
- ambiguous Complete 先 HEAD deterministic key，并验证 size 和返回的 VersionId/strong ETag；已完成则进入
  PUBLISHING，否则重试 Complete。
- publish 前失败保留 job 和 staging；明确取消进入 ABORTING，Abort 成功或 NoSuchUpload 后才能清理 staging。

凭证错误为人工可恢复失败，429/5xx/timeout 使用有界指数退避和 jitter；取消可打断等待和 staging read。

## 7. 原子 publish

`PublishOriginFileFromStaging` 在单个 FDB 事务中：

1. 校验 job、owner、stateVersion、expected staging inode 和 SEALED length；
2. 校验 completed object identity、size、目标 CACHE_DATA layout；
3. 创建新的 OriginFile inode；
4. 将 path dentry 从 staging inode CAS 替换为新 inode；
5. 保存 PUBLISHED job result，并把 staging inode 交给普通 inode GC。

事务超时后使用 jobId 查询；不得重复创建第二个 OriginFile。旧 open handle 仍引用 staging inode，但 seal 后只读，
最后一个 session 关闭后才回收。publish 成功后提交高优先级 PrefetchJob；prefetch 失败只影响 warm 状态，不回滚
已经持久化和发布的文件。

FUSE `fsync`、flush 和可报告错误的 release 等待 PUBLISHED。若应用从不调用同步点且进程退出，后台恢复仍可
完成对象和 namespace，但该进程不能被告知最终错误。

## 8. 升级、回滚和配置

升级顺序：Storage schema/role 与 inventory RPC → Metadata schema/operations → Cache Manager workers → Client/FUSE
write routing → enable Phase 4。启用前必须完成一次 dry-run reconcile，确认没有 unknown newer generation。

回滚先禁止新 staging，等待或取消所有非终态 upload，完成 reconcile，确认无 WRITE_STAGING inode/job 和无
Phase-4-only mutation，再关闭 feature。已发布 OriginFile 沿用前三阶段兼容规则。

代表配置：

```toml
enable_phase4 = false
loading_recovery_interval = "10s"
reconcile_interval = "10m"
reconcile_page_size = 256
reconcile_max_concurrency = 8
reconcile_dry_run = false

write_staging_table_id = 0
upload_part_size = "16MiB"
upload_max_parallel_parts = 4
upload_retry_limit = 8
writer_lease_ttl = "60s"
abandoned_staging_policy = "resume"
```

## 9. 验收标准

- 过期 LOADING 无需新 miss 即可被 fence、清理并重排，旧 Loader 的延迟 Commit/Fail 无效；
- Metadata→Storage 能发现 READY 丢块，Storage→Metadata 能清理 orphan/older generation；
- inventory、event、cleanup 和 eviction 重放不会重复释放物理或逻辑状态；
- Cache Manager 在 S3 GET、3FS write、Metadata Commit 和 reconcile 任一边界退出后最终收敛；
- 新文件顺序写经 fsync/close 后对象存在、path 指向匹配对象身份的 OriginFile，并能重新读取；
- multipart 任一 part、Complete 或 publish 超时都可幂等恢复；未完成对象永不发布；
- staging 不受 cache eviction 影响，完成/取消后最终回收；
- admission 仍只依据物理容量，eviction policy 仍可配置且默认 LRU；
- Phase 1–3、普通 3FS 文件和真实 MinIO 回归不退化。
