# 3FS Cache 第四阶段可靠性恢复与写入实施计划

## 1. 使用方式

本计划把[第四阶段设计](../specs/2026-08-07-phase-4-reliability-write-design.md)拆成 36 个任务（T0..T35）。
实施顺序先补恢复协议和库存闭环，再引入 WRITE_STAGING、multipart 和原子发布。Phase 4 始终默认关闭，
每个任务使用最窄测试；一个大功能完成后再做一次增量编译。

不得修改 `third_party/`、生成文件或用户既有改动。任何 recovery mutation 都必须复用 generation/epoch fence；
任何上传 mutation 都必须以 uploadJobId/stateVersion 幂等。

## 2. 依赖图

```text
M0 基线              T0
M1 恢复协议          T0 -> T1 -> T2 -> T3
M2 Loader 恢复       T3 -> T4 -> T5 -> T6 -> T7
M3 双向 Reconcile    T2/T7 -> T8 -> T9 -> T10 -> T11 -> T12 -> T13 -> T14
M4 Manager Recovery  T7/T14 -> T15 -> T16 -> T17
M5 Staging 地基       T1/T17 -> T18 -> T19 -> T20 -> T21 -> T22
M6 Multipart         T22 -> T23 -> T24 -> T25 -> T26
M7 Publish           T20/T26 -> T27 -> T28 -> T29 -> T30 -> T31
M8 运维与验收         T14/T31 -> T32 -> T33 -> T34 -> T35
```

## 3. M0：基线

### T0：冻结 Phase-4 基线

- 新增 `docs/dev/cache-phase-4-baseline.md`，记录 Phase-3 acceptance commit、分支、dirty worktree 和环境。
- 固定 cache/meta/storage/cache-manager/FUSE/Admin/MinIO 回归矩阵。
- 明确外部服务缺失是环境失败，服务启动后 assertion/timeout 是功能失败。

测试：执行现有 focused discovery，记录可复现命令和起始计数。

建议提交：`test(cache): record phase four baseline`

## 4. M1：协议和持久化边界

### T1：追加 Phase-4 capability 与公共 identity

- 增加 Phase-4 protocol/schema capability、UploadJobId、UploadJobState、CompletedPart 和 reconcile 类型。
- 追加字段，不改变已有 method ID、enum 数值或旧 payload 解释。
- 校验路径、对象 key、part 数/大小、stateVersion 和错误文本上限。

测试：serde round-trip、旧 payload 默认值、非法状态组合和 method ID 唯一性。

建议提交：`feat(cache): define phase four identities`

### T2：定义 Cache Inventory wire contract

- 在 Storage service 尾部追加分页 inventory RPC，在 Metadata 尾部追加批量 reconcile lookup。
- cursor opaque，响应携带 inventory epoch、done 和有界 entries。
- 请求显式 target，Storage 校验 CACHE_ONLY role。

测试：limit、cursor、role、epoch、descriptor 缺失和协议 gate。

建议提交：`feat(storage): define cache inventory contracts`

### T3：扩展恢复扫描 contract

- RecoverableCachePermit 增加 lease deadline、generation 和 placement。
- 定义 expired-LOADING fenced recovery request/result。
- 旧 Phase-2 caller 不发送新字段时 fail closed，不猜测 placement。

测试：QUEUED/LOADING 组合、旧 fence、过期边界、重复请求。

建议提交：`feat(meta): define loading recovery contracts`

M1 退出：协议可编译、Phase 4 默认关闭、没有外部 mutation 路径。

## 5. M2：完整 Loader lease 恢复

### T4：Metadata 分页列出完整 LOADING

- CacheBlockStore 增加稳定分页过滤，不再通过 `snapshotListAll` 全量排序。
- 返回 loader/epoch/lease/generation/permit/placement 的冻结快照。
- cursor 推进和空页规则明确。

测试：混合状态、页边界、并发删除/插入、缺字段损坏记录。

建议提交：`feat(meta): page recoverable cache loads`

### T5：实现 fenced expired-LOADING transition

- 新增或扩展 FailCacheBlocks，使 recovery 只能匹配原 loaderId/loadEpoch 和 lease deadline。
- 原子进入 CLEANING、递增 fence、保留 generation/placement/charge，terminalState 设为 REENQUEUE 或 FAILED。
- 重复调用返回幂等结果；旧 Loader Commit/Fail 为 no-op/conflict。

测试：deadline 两侧、双恢复者、旧 commit、charge 只保留一次。

建议提交：`feat(meta): fence expired cache loads`

### T6：扩展 PermitRecovery 处理 LOADING

- 查询 permit 和 exact placement；有效未过期 PINNED 延后，缺失/过期进入 T5。
- 调用现有 cleanup worker 清理 generation，再释放 permit。
- retryable Storage/Metadata 错误保留状态并重试。

测试：PINNED、RESERVED、missing、timeout、cleanup partial failure。

建议提交：`feat(cache-manager): recover loading leases`

### T7：加入周期 lease recovery worker

- startup barrier 先跑一轮，之后按配置周期扫描。
- 与正常 loader、cleanup 和 stop/drain 正确协调。
- 暴露 recovered/deferred/failed 指标。

测试：启动失败回滚、重复 start/stop、恢复中取消和并发 acquire。

建议提交：`feat(cache-manager): run loading recovery worker`

M2 退出：过期 LOADING 不依赖新 miss 即可最终清理和重排。

## 6. M3：双向 Reconciler

### T8：Storage 实现 cache inventory 分页

- 从 cache descriptor 持久化索引生成 entry，过滤 retired 状态并保留 generation。
- 仅允许 CACHE_ONLY target；WRITE_STAGING 和 USER_DATA 永不出现。
- epoch/cursor 对目标重启和并发 mutation fail closed。

测试：多页、空目标、epoch 改变、错误 role、旧新 generation 并存。

建议提交：`feat(storage): page physical cache inventory`

### T9：StorageClient 与 CacheManager backend 接入 inventory

- 路由到明确 node/target，限制 batch、timeout 和并发。
- 对 malformed page、重复 cursor、错误 target 返回 InvalidResponse。

测试：路由刷新、节点失败、重复 token、partial target failure。

建议提交：`feat(cache-manager): query cache inventory`

### T10：Metadata 实现 reconcile lookup batch

- 按 CacheBlockKey 返回完整 record 摘要或明确 NONE。
- service identity、Phase-4 gate、batch 上限和 snapshot 一致性。

测试：所有状态、缺记录、权限、重复 key。

建议提交：`feat(meta): expose reconcile cache state`

### T11：实现 MetadataToStorageChecker

- 分页 READY/EVICTING/CLEANING，按持久化 placement 查询 generation/descriptor。
- missing/mismatch 进入现有 INVALID/CLEANING；不可达不 mutation。
- 与 event/report-invalid 幂等收敛。

测试：全匹配、单 replica 丢失、checksum/length mismatch、旧 event race。

建议提交：`feat(cache-manager): reconcile metadata to storage`

### T12：实现 StorageToMetadataChecker

- 遍历每个 CACHE_ONLY target inventory，批量 lookup Metadata。
- orphan/older generation 走 fenced retire；newer generation 只记录 conflict。
- exact LOADING/EVICTING/CLEANING 委派对应 owner，不重复完成。

测试：矩阵全覆盖、同 block 多 replica、扫描后新写入和 generation race。

建议提交：`feat(cache-manager): reconcile storage to metadata`

### T13：实现 CacheReconciler 调度与背压

- 组合双向 checker，限制 page、target concurrency、每轮 mutation 和运行时间。
- epoch 改变重扫；单 target 失败不阻止其他 target，最终状态保留失败摘要。
- 支持 dry-run。

测试：公平性、背压、stop、重入拒绝、部分错误汇总。

建议提交：`feat(cache-manager): coordinate cache reconciliation`

### T14：Reconcile metrics、status 与审计记录

- 记录 last start/success、scanned/orphan/missing/conflict/repaired/retryable。
- 不记录 bucket 凭证、对象签名或无限 cardinality key。
- status 区分 never-run、running、healthy、degraded。

测试：counter 精确、错误脱敏和 restart reset/persist 边界。

建议提交：`feat(cache): observe reconciliation health`

M3 退出：Metadata 与物理 cache inventory 可双向核对并安全修复。

## 7. M4：Cache Manager 恢复屏障

### T15：统一 startup recovery coordinator

- 固定 routing→permit/loading→eviction/cleanup→jobs→reconcile 的顺序。
- admission 在屏障完成前保持关闭；read fallback 不受影响。
- 任一步失败执行逆序 stop/cleanup。

测试：每个边界注入失败、启动重试、无 worker 泄漏。

建议提交：`feat(cache-manager): coordinate startup recovery`

### T16：恢复未完成 cleanup、eviction 和 job ownership

- 审计并补齐 CLEANING、EVICTING、PrefetchJob 的分页重放。
- 相同 operation identity 重试；不使用当前 routing 替换旧 placement。

测试：进程在 mutation 前后退出、重复恢复、route change。

建议提交：`fix(cache-manager): resume durable cache work`

### T17：Phase-4 rollout gate 与 drain

- mgmtd capability、Cache Manager status 和 Admin gate 加入 recovery/reconcile 健康条件。
- enable 前 dry-run 无 newer conflict；rollback 前无 nonterminal recovery work。

测试：升级顺序、旧节点、drain timeout、回滚拒绝。

建议提交：`feat(mgmtd): gate cache phase four recovery`

M4 退出：Manager 重启前后所有已有缓存控制工作最终收敛。

## 8. M5：Write staging 地基

### T18：增加 WRITE_STAGING chain role

- 扩展 mgmtd/storage role、配置和路由校验。
- 普通用户 File 仅在 write-through create 操作中可引用该 role。
- cache inventory、admission、normal/local eviction 明确排除 staging。

测试：角色矩阵、错误 I/O、rollout 和普通 3FS 回归。

建议提交：`feat(storage): add write staging role`

### T19：定义并持久化 UploadJobStore

- 分配新 FDB key prefixes，记录 keyspace 文档。
- create/load/list/CAS update，parts 有界并校验状态机。
- jobId 幂等，不同 spec 重用冲突。

测试：serde、并发 CAS、分页、part 上限、损坏 record。

建议提交：`feat(meta): persist write upload jobs`

### T20：原子创建 staging path 和 job

- 专用 create operation 同事务创建 dentry、File inode 和 UploadJob。
- 第一版只允许不存在 path、单 writer、顺序写配置。
- timeout 以 caller jobId 重试并返回原 inode/job。

测试：重复请求、路径冲突、权限、错误 chain role、事务冲突。

建议提交：`feat(meta): create write staging files`

### T21：实现 writer lease 与 seal

- 写入续租 lease；seal 冻结 inode version/length 并禁止后续 write/truncate/hole。
- fsync/close 重试返回同一 sealed result。
- lease 到期按配置恢复或取消。

测试：并发 writer、过期、seal/write race、长度变化和重试。

建议提交：`feat(meta): seal staged uploads`

### T22：Client/FUSE 写路由接入 staging

- Phase-4 create 选择 WRITE_STAGING，继续复用现有 3FS write pipeline。
- 拒绝随机 offset、append reopen、mmap write 和 OriginFile overwrite。
- fsync/flush/release 调用 seal 并等待 publish；普通 File 路径不变。

测试：顺序写、非法语义、错误传播、普通 FUSE/native 回归。

建议提交：`feat(fuse): route write through staging`

M5 退出：新文件可安全写入并 seal，但尚不上传或发布。

## 9. M6：Multipart upload

### T23：扩展 ObjectStore multipart 抽象

- 增加 create/uploadPart/complete/abort/head-completed vendor-neutral types。
- RoutedObjectStore 和 fake 全部实现；公共接口不泄露 AWS SDK 类型。

测试：validation、routing、cancellation 和错误分类。

建议提交：`feat(cache): define multipart object uploads`

### T24：实现 AWS S3 multipart executor

- 映射 CreateMultipartUpload、UploadPart、CompleteMultipartUpload 和 AbortMultipartUpload。
- 校验 ETag/VersionId/size，凭证和 request body 不进日志。
- MinIO endpoint override 沿用现有配置。

测试：mock executor、HTTP 错误映射、malformed response。

建议提交：`feat(cache): upload multipart objects to s3`

### T25：实现 MultipartUploader checkpoint

- 从 frozen staging inode 连续读取，按 part 上传并 CAS 保存 checkpoint。
- 相同 part number 幂等重试，校验 part count/size/checksum。
- 429/5xx/timeout 退避，auth/invalid response 停止自动重试。

测试：每个 part 前后 crash、重复 part、尾 part、取消和 retry budget。

建议提交：`feat(cache-manager): checkpoint multipart uploads`

### T26：实现 Complete/Abort 恢复

- ambiguous Complete 通过 HEAD deterministic key 验证并收敛。
- cancel/terminal failure 持久化 ABORTING，NoSuchUpload 视为幂等成功。
- completed object identity 写入 job，未验证不得进入 publish。

测试：Complete timeout、已完成 HEAD、Abort timeout、对象不匹配。

建议提交：`feat(cache-manager): recover multipart completion`

M6 退出：sealed staging 可可靠成为完整 S3 object，但 namespace 尚未替换。

## 10. M7：原子发布和生命周期

### T27：定义 PublishOriginFileFromStaging contract

- 请求包含 service identity、job/stateVersion、expected staging inode、completed object 和 CACHE_DATA layout。
- 响应返回 published inode 和幂等 outcome。

测试：wire、权限、旧协议、错误 identity/layout。

建议提交：`feat(meta): define staged publish contract`

### T28：实现原子 dentry publish

- 单事务创建 OriginFile、CAS 替换 dentry、更新 UploadJob PUBLISHED。
- timeout 重试只返回同一 inode；path/inode 改变返回 conflict。
- staging inode 交给普通 GC，不在事务外直接删除。

测试：事务边界 crash、重复 publish、open staging handle、并发 rename/remove。

建议提交：`feat(meta): publish staged origin files`

### T29：实现 WritePublishController

- 驱动 SEALED→UPLOADING→COMPLETING→PUBLISHING→PUBLISHED。
- 分页恢复非终态 jobs，按 owner/origin 限制并发。
- stop/drain 不启动新 part，并保存可恢复状态。

测试：每个状态 crash、多个 job、公平性、取消和 restart。

建议提交：`feat(cache-manager): publish write through jobs`

### T30：接入 fsync/close 等待与结果查询

- Client 按 jobId 等待 terminal，支持 deadline/cancellation。
- PUBLISHED 才成功；FAILED/CANCELLED 返回稳定错误。
- FUSE release 无法被应用观察的限制写入 runbook，fsync 保证可观察。

测试：成功、timeout 后查询、Client 重连、错误 errno 映射。

建议提交：`feat(client): await write through publish`

### T31：发布后预热与 staging GC

- publish 后创建高优先级 PrefetchJob；失败不回滚已发布文件。
- 最后 open handle 关闭后回收 staging chunks/inode；重复 GC 幂等。
- orphan object/staging 的保留和清理策略可查询。

测试：prefetch failure、GC race、Manager restart 和 cleanup retry。

建议提交：`feat(cache-manager): warm published origin files`

M7 退出：新文件从普通 path 写入、持久化、原子发布并可重新读取。

## 11. M8：运维、集成与验收

### T32：Admin reconcile/upload 命令

- 增加 cache-reconcile run/status/dry-run 和 cache-upload status/cancel/retry。
- 默认分页、明确确认 destructive repair，输出不含凭证。

测试：CLI parsing、权限、JSON/table 输出和 timeout。

建议提交：`feat(admin): operate cache recovery and uploads`

### T33：配置、metrics、runbook 与 rollout 文档

- 示例配置加入全部 Phase-4 开关、interval、page、part、lease 和 staging table。
- 指标覆盖 recovery/reconcile/upload/publish/GC；更新 upgrade/drain/rollback runbook。

测试：配置默认关闭、边界 validation、指标注册唯一。

建议提交：`docs(cache): document phase four operations`

### T34：故障注入和真实 MinIO 集成

- 覆盖 Loader S3 GET、3FS write、Metadata Commit、inventory scan、每个 multipart part、Complete 和 publish crash。
- MinIO 验证真实 multipart、HEAD recovery、abort、重新读取和 staging 清理。
- 普通 CI 缺环境必须明确 SKIPPED，不冒充通过。

测试：窄故障矩阵和 credentialed MinIO target。

建议提交：`test(cache): exercise phase four recovery`

### T35：全阶段验收、提交与推送

- 增量构建 cache/meta/storage/cache-manager/client/FUSE/Admin 和真实 MinIO target。
- 运行 Phase 1–4 focused suites、format、`git diff --check`，记录准确计数和环境限制。
- 新增 `docs/dev/cache-phase-4-acceptance.md`，确认 feature 默认关闭并推送 branch。

建议提交：`test(cache): close phase four acceptance`

M8 退出：设计验收项全部有实现和自动化证据，远程分支包含完整可审计提交链。
