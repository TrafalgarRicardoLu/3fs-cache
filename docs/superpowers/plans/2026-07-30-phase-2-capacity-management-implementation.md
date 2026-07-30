# 3FS Cache 第二阶段容量管理实施计划

## 1. 目的与使用方式

本计划把已批准的[第二阶段容量管理设计](../specs/2026-07-30-phase-2-capacity-management-design.md)拆成 31
个可独立实现、测试和提交的任务。实施顺序优先关闭物理容量与删除一致性风险，再接入近似热度和策略。

每个任务完成时应：

1. 只修改任务列出的职责范围，不顺手重构；
2. 先补失败测试，再实现，再运行最窄相关测试；
3. 运行 `git diff --check` 并检查没有混入用户改动；
4. 使用任务建议的聚焦提交主题；
5. 里程碑结束后运行该里程碑的组合测试；
6. P0 安全断言不能用重试、告警或后续 Reconciler 代替。

性能不是第二阶段的验收门槛，但所有上报、准入和驱逐控制面工作不得阻塞前台 Origin fallback。

## 2. 已确认的实现约束

- 第二阶段只支持一个 Cache Manager。
- CACHE_ONLY 是物理磁盘级持久化角色，同盘所有 Target 角色相同。
- Manager 空间快照只用于预筛选；Storage `CacheSpaceGate` 是物理准入权威。
- Metadata 逻辑容量只提供查询，不拒绝 Enqueue。
- READY 必须持久化实际写入的 `PlacementIdentity`，驱逐不能从当前路由重建 placement。
- LRU 是第一个可配置策略实现，控制器不得写死 LRU。
- EVICTING 使用新 coordinated retire 与 Storage Event；Phase-1 CLEANING 暂保留原同步完成所有权。
- PREPARED operation 不占用事件 sequence；sequence 只在 DELIVERABLE 事务中分配。
- 升级到第二阶段前必须排空 Phase-1 缓存数据。
- 多 Manager、全量 inventory reconcile、完整 LOADING recovery 仍属于第四阶段。

## 3. 依赖图

```text
M0 协议与专用磁盘地基
T0 -> T1 -> T4
       ├-> T2 -> T3
       └-> T5

M1 物理准入闭环
T5 -> T6 -> T7
T1/T4 -> T8 -> T9
T7/T8/T10/T11 -> T12 -> T13 -> T14

M2 访问热度
T4 -> T15
T8 -> T16
T15/T16 -> T17
T13 -> T18

M3 正常驱逐
T16/T17/T18 -> T19
T8/T19 -> T20
T11/T19/T20 -> T21 -> T22

M4 可靠删除与本地安全兜底
T1/T2/T4 -> T23 -> T24
T13/T23 -> T25 -> T26
T20/T21/T24/T26 -> T27

M5 运维与升级
T3/T14/T17/T21/T24/T27 -> T28 -> T29

M6 验收
T0..T29 -> T30
```

## 4. 建议提交与验证策略

- 每个任务默认一个提交；协议与实现 diff 过大时可拆成连续两个提交。
- 不修改 `third_party/`，除非任务明确变成依赖升级任务。
- 新 wire method ID 必须追加，不能复用已发布 ID。
- 新 KV prefix 必须先做冲突审计。
- 新增配置默认关闭 Phase 2，直到 T29 的 enable 前置检查完成。

通用窄测试命令：

```bash
cmake --build build --target test_common test_mgmtd test_meta test_storage_store \
  test_storage_service test_cache_manager test_client -j 32
ctest --test-dir build -R \
  '^(test_common|test_mgmtd|test_meta|test_storage_store|test_storage_service|test_cache_manager|test_client)$' \
  --output-on-failure
```

## 5. M0：协议、身份与专用磁盘地基

### T0：冻结第二阶段基线与测试矩阵

涉及文件：

- 新增 `docs/dev/cache-phase-2-baseline.md`
- `tests/CMakeLists.txt`
- 现有 cache/meta/storage/mgmtd/client 测试入口

实施：

- 记录当前分支、Phase-1 相关提交和工作区非本任务改动。
- 记录现有 Cache、Metadata、Storage、Mgmtd、Client、MinIO 测试结果。
- 建立第二阶段测试标签或过滤规则，不新增空占位测试。
- 把当前 8 个环境/基线失败与缓存功能失败分开记录。

完成条件：基线文档可复现，后续每个里程碑都能与该基线比较。

建议提交：`test(cache): record phase two baseline`

### T1：定义 Phase-2 公共身份、状态与协议版本

涉及文件：

- `src/fbs/cache/Common.h`
- `src/fbs/storage/Common.h`
- 公共错误码注册文件
- `tests/common/cache/`

实施：

- 定义 `PhysicalDiskId`、`StorageRole`、`PlacementIdentity`、`PermitIdentity`。
- 定义 `EvictionEpoch`、`EvictionReason`、`CacheStorageEventType`、事件/permit 状态枚举。
- `PlacementIdentity` 包含 versioned chain、排序去重 replica set、coordinator Target 和 admission attempt ID。
- 增加稳定错误：RoleMismatch、PermitExpired、PermitConflict、PlacementMismatch、EventGap、JournalFull。
- cache schema/protocol capability 升级到 Phase-2 版本。

测试：serde round-trip、排序与去重、coordinator 必须属于 replica set、空身份与溢出拒绝、旧协议拒绝。

建议提交：`feat(cache): define phase two capacity identities`

### T2：持久化 Storage 磁盘与 Target 角色

涉及文件：

- `src/storage/store/StorageTargets.*`
- `src/storage/store/StorageTarget.*`
- Storage target/disk 初始化元数据
- `src/storage/service/TargetMap.*`
- `tests/storage/store/`

实施：

- 初始化物理磁盘时生成并持久化稳定 `PhysicalDiskId` 和 `StorageRole`。
- 同一磁盘所有 Target 继承相同角色；有数据时禁止静默改角色。
- 启动时扫描已有 Target/chunk；角色不一致则 fail closed。
- routing 更新如果把 USER_DATA chain 放到 CACHE_ONLY Target，或反向放置，拒绝服务并报警。
- 明确旧磁盘缺少角色元数据时的迁移拒绝信息。

测试：重启稳定性、同盘多 Target、旧元数据、已有数据改角色、错误 routing 更新。

建议提交：`feat(storage): persist cache-only disk roles`

### T3：Mgmtd 强制执行磁盘角色隔离

涉及文件：

- `src/fbs/mgmtd/HeartbeatInfo.h`
- `src/fbs/mgmtd/TargetInfo.h`
- `src/mgmtd/ops/HeartbeatOperation.cc`
- chain/table create、attach、update 操作
- `tests/mgmtd/`

实施：

- Storage heartbeat 上报 Target、PhysicalDiskId 和 role。
- Mgmtd 持久化 Target 到磁盘/角色映射。
- 未知磁盘身份或角色时拒绝相关 chain mutation。
- create/attach/upload/update 时验证 CACHE_DATA 与 USER_DATA 的物理磁盘集合完全不相交。
- 现有同盘混用配置在 Phase-2 enable 前返回明确诊断。

测试：同盘不同 Target 混用、角色缺失、heartbeat 变化、routing update 回滚、合法 cache-only table。

建议提交：`feat(mgmtd): enforce physical cache target isolation`

### T4：固定 Phase-2 wire contract 与 capability gate

涉及文件：

- `src/fbs/meta/Service.h`
- `src/fbs/storage/{Cache,Service}.h`
- `src/fbs/cache_manager/{Common,Service}.h`
- 对应 Meta/Storage/Cache Manager stubs 和 clients
- capability gate 与 mock service

实施：

- 增加 Access Report、空间/footprint、permit prepare/renew/release/query、BeginEvict/ListEvicting、access update、
  coordinated retire、Storage Event report/ACK、dead-letter/status RPC。
- 所有批量请求有硬上限、逐项结果和输入顺序保证。
- method ID 只追加，Phase-1 method 不改号。
- Phase-2 enable flag 默认关闭；不兼容组件返回 UpgradeRequired/FeatureDisabled。
- 更新 mock 和 in-memory client，使后续任务能写窄测试。

测试：serde、valid、batch limit、method ID 唯一、旧协议与未 enable 行为。

建议提交：`feat(cache): define phase two service contracts`

M0 退出条件：磁盘角色可执行、公共身份稳定、wire contract 可编译，Phase 2 仍默认关闭。

## 6. M1：物理容量准入闭环

### T5：实现 Storage 物理容量与 footprint 口径

涉及文件：

- `src/fbs/storage/Common.h`
- `src/storage/store/{StorageTargets,StorageTarget,ChunkStore,ChunkEngine}.*`
- `src/storage/service/StorageOperator.*`
- `tests/storage/store/TestCachePhysicalCapacity.cc`

实施：

- 计算 `cacheCapacityBytes`、`cachePhysicalUsedBytes`、`cacheAllocatableBytes`、`cacheReservedBytes`。
- 口径包含预分配、保留区域、可回收但尚未回收空间和 filesystem availability。
- 实现 `physicalFootprint(target, chunkSize, payloadLength)`；短尾块按真实 allocation unit 返回。
- `SpaceInfo` 返回稳定 disk ID、role、Target 列表和 Storage 强制 high watermark。
- remote sampledAt 只用于诊断。

测试：短块、完整块、不同 engine、预分配、回收前后、同盘多个 Target 不重复容量。

建议提交：`feat(storage): report cache physical capacity`

### T6：实现持久化每盘 CacheSpaceGate

涉及文件：

- 新增 `src/storage/cache/space/{CacheSpaceGate,CacheSpacePermitStore}.*`
- Storage 本地 KV prefix/column family
- `tests/storage/cache/TestCacheSpaceGate.cc`

实施：

- 以 PermitIdentity 持久化每个 Target footprint、过期时间和状态。
- 在同一磁盘锁/事务下检查 role、水位、allocatable 和所有活跃 permit 后 reserve。
- prepare、renew、release、consume 对同一 identity 幂等。
- expired permit 禁止新 replace；executing permit 必须 pin 到写完成。
- journal/permit 元数据达到上限时 fail closed。

测试：并发 reserve 不能越过 high、expiry、renew、consume、重复 release、执行中跨 expiry、重启恢复。

建议提交：`feat(storage): gate cache writes with durable permits`

### T7：实现多副本 permit prepare 协调器

涉及文件：

- `src/storage/service/StorageOperator.*`
- `src/client/storage/StorageClient*`
- 新增 Storage cache permit coordinator
- `tests/storage/service/TestCachePermitCoordinator.cc`

实施：

- 使用 PlacementIdentity.coordinatorTargetId 作为确定性协调者。
- 对 recorded replica set 逐 Target 获取 permit；同盘多个副本分别占 footprint。
- 部分成功时用相同 attempt ID 回滚；超时重试不重复 reservation。
- replace 请求必须携带完整 PermitIdentity，所有 replica 校验一致。
- query 返回旧 Manager epoch permit 和 executing write，供恢复使用。

测试：第二个 replica 拒绝、同盘双副本、协调者崩溃、重复 prepare、部分回滚失败后恢复。

建议提交：`feat(storage): coordinate replicated cache permits`

### T8：扩展 Metadata 记录以持久化 Permit 与 Placement

涉及文件：

- `src/meta/store/cache/CacheBlockRecord.*`
- `src/meta/store/cache/CacheBlockStore.*`
- Enqueue/Acquire/Commit/Fail/BeginClean ops
- `tests/meta/cache/TestCachePermitState.cc`

实施：

- QUEUED/LOADING 持久化 PermitIdentity；READY 持久化不可变 PlacementIdentity。
- Enqueue 以 admissionAttemptId 幂等，响应区分 CREATED/QUEUED/LOADING/READY。
- Permit generation 替换使用 state、旧 permit、loader lease CAS。
- Commit 校验 replace/permit/placement 完全一致，清 permit 并固化 placement。
- 进入 CLEANING 时先把 permit placement 提升为 cleanup placement。
- Record valid() 覆盖所有状态与字段组合。

测试：状态矩阵、重复 attempt、错误 placement commit、permit CAS 竞争、LOADING/CLEANING placement 保留。

建议提交：`feat(meta): persist cache permit and placement identity`

### T9：把 Metadata 逻辑容量改为只读统计口径

涉及文件：

- `src/meta/store/cache/CacheCapacityStore.*`
- cache status response/CLI 展示
- `tests/meta/cache/TestCacheCapacity.cc`

实施：

- reserve 只做计数和溢出检查，不比较 logicalCapacity。
- valid() 移除 `used <= logicalCapacity`；设置 reference capacity 不因当前 used 更大而失败。
- 保留 RESERVED/COMMITTED exact-once 转换与释放。
- status 明确 logical 字段 informational，不计算为准入 free space。

测试：used 超 reference 仍合法、并发统计一致、cleanup/eviction 只释放一次、CLI 不误导。

建议提交：`refactor(meta): make cache capacity informational`

### T10：建立可插拔 Admission Policy 与 second-miss 实现

涉及文件：

- 新增 `src/cache_manager/admission/{AdmissionPolicy,SecondMissAdmissionPolicy,AdmissionPolicyFactory}.*`
- `src/cache_manager/config/Config.h`
- `tests/cache_manager/TestAdmissionPolicy.cc`

实施：

- AdmissionPolicy 接收不可变 context，返回 ADMIT/BYPASS 与 reason。
- SecondMiss 使用有界内存 window；首次 miss bypass，窗口内第二次 admit。
- 过期和容量上限清理确定、可测试。
- 未知策略名启动失败；预留未来策略注册接口。

测试：首次/第二次、过期、乱序重复、上限淘汰、重启清空、未知策略。

建议提交：`feat(cache-manager): add second-miss admission policy`

### T11：实现 Manager 物理拓扑、快照与预筛选

涉及文件：

- 新增 `src/cache_manager/capacity/{PhysicalTopology,SpacePoller,PhysicalPreflight}.*`
- `src/cache_manager/loader/CacheLoaderBackend.*`
- `tests/cache_manager/TestPhysicalCapacity.cc`

实施：

- 组合 RoutingInfo 与 Storage SpaceInfo：chain→replica→node→disk。
- Manager 用请求开始/接收的 monotonic time 判断 freshness。
- 同盘多 replica footprint 累加，disk capacity 只计一次。
- high watermark 配置必须与 Storage advertised enforced value 一致。
- 缺 target、role、disk、space 或 stale snapshot 时只暂停受影响 chain。
- 内存 preflight reservation 仅优化并发 RPC，不作为正确性来源。

测试：共享盘、副本倾斜、过期快照、路由缺失、水位不一致、无关 chain 继续准入。

建议提交：`feat(cache-manager): track physical cache capacity`

### T12：把 EnsureCached 改为 second-miss 物理准入入口

涉及文件：

- `src/cache_manager/service/EnsureCached.*`
- scheduler/hint coalescer
- Cache Manager backend Meta/Storage calls
- `tests/cache_manager/TestEnsureCached.cc`

实施：

- 现有 EnsureCached 成为唯一 foreground miss signal。
- 第一次 miss 不调用 Metadata Enqueue；第二次依次执行 policy、preflight、Storage permit、Metadata Enqueue。
- READY 立即释放新 permit；QUEUED 按有效性 attach/renew/CAS replace；LOADING 遵守 executing fence。
- ambiguous Enqueue 用相同 attemptId 重试。
- scheduler attach 失败调用 fenced CancelQueuedAdmission，再释放 Storage permit。
- 所有 bypass 不影响已成功的 Origin read。

测试：从真实 CacheReadPipeline 发起，首次 miss 后 Metadata 仍 NONE；第二次创建 QUEUED；各失败点无 permit 泄漏。

建议提交：`feat(cache-manager): admit cache loads by physical space`

M1 退出条件：并发和重启下都不能越过 Storage high watermark，首次 miss 不建记录，逻辑容量不参与准入。

## 7. M2：Loader placement、访问热度与本地描述

### T13：把 Permit/Placement 接入 Loader、replace 与 Commit

涉及文件：

- `src/cache_manager/loader/CacheLoader.*`
- `src/fbs/storage/Cache.h`
- `src/storage/service/StorageOperator.*`
- `src/storage/store/{ChunkEngine,ChunkReplica}.*`
- `tests/cache_manager/TestCacheLoader.cc`
- `tests/storage/cache/TestCacheDescriptor.cc`

实施：

- Loader 持有并续租 PermitIdentity，replace 前确认未过期。
- replace 在每个 replica 校验 permit 并持久化完整 CacheChunkDescriptor。
- descriptor 包含 logical key、generation、完整 placement、target、createdAt、lastAccessAt。
- Commit 携带同一 placement；任何差异 fail fenced cleanup。
- write commit 消费 permit，失败/取消释放；partial replace 保留 cleanup placement。

测试：短块 footprint、permit 过期、placement mismatch、部分 replica 成功、Commit timeout、descriptor 重启恢复。

建议提交：`feat(cache): bind cache loads to physical placement`

### T14：实现 Permit 生命周期恢复与 QUEUED 取消

涉及文件：

- Cache Manager startup/recovery worker
- Metadata `CancelQueuedAdmission` 和 permit CAS op
- Storage permit query/renew/release
- `tests/cache_manager/TestPermitRecovery.cc`
- `tests/meta/cache/TestQueuedAdmission.cc`

实施：

- Manager 启动生成新 epoch，加载旧 epoch permit/executing inventory，完成前停止准入。
- 扫描 QUEUED：有效 permit 续租，无效 permit prepare + CAS replace，竞争失败释放新 permit。
- scheduler failure 原子取消精确 QUEUED attempt 并释放逻辑统计。
- executing LOADING 禁止换 permit；非 executing LOADING 只在现有 lease/miss fenced 路径恢复。
- 明确扫描 cursor 可从头重放且操作幂等。

测试：enqueue 后崩溃、schedule 前崩溃、permit expiry、两个恢复者竞争、执行中跨 Manager restart。

建议提交：`feat(cache-manager): recover physical cache permits`

### T15：实现 Client READY hit 异步批量上报

涉及文件：

- 新增 `src/client/cache/CacheAccessReporter.*`
- `src/client/cache/CacheReadPipeline.*`
- Cache Manager client/stub
- Client/FUSE 配置
- `tests/client/cache/TestCacheAccessReporter.cc`

实施：

- READY 读取校验成功后记录 key、generation 和诊断时间。
- threshold/interval 异步 flush；buffer 有界，满或发送失败丢弃并计数。
- 不在该接口重复发送 Origin miss；miss 仍由 EnsureCached 表达。
- reporter shutdown 有界，不阻塞前台 read。

测试：批量阈值、周期、乱序 hit、buffer overflow、Manager down、shutdown、前台非阻塞。

建议提交：`feat(client): report cache hits asynchronously`

### T16：持久化 READY 热度摘要

涉及文件：

- `src/meta/store/cache/CacheBlockRecord.*`
- 新增 `UpdateCacheBlockAccess` op
- Meta service/client
- `tests/meta/cache/TestCacheAccess.cc`

实施：

- READY commit 初始化 readyAt/lastAccessAt。
- 批量 update 使用 key + generation CAS，只做 `max(lastAccessAt, managerReceiveTime)`。
- 旧 generation、非 READY、重复/乱序报告安全 no-op。
- list/status 返回 eviction 所需摘要，不把 Client wall clock 当权威。

测试：代际变化、乱序、重复、EVICTING race、批量部分失败。

建议提交：`feat(meta): persist cache access recency`

### T17：实现 Manager Access 聚合器

涉及文件：

- 新增 `src/cache_manager/access/{AccessAggregator,AccessFlushWorker}.*`
- Cache Manager service/operator
- `tests/cache_manager/TestAccessAggregator.cc`

实施：

- 按 block/generation 合并 READY hit，只保留 Manager receive time 最大值。
- 周期/阈值批量写 Metadata。
- 聚合结构有界；过载允许丢近似热度，不影响状态机。
- stale generation 结果计数，不做同步重试阻塞。

测试：合并、generation 分桶、容量淘汰、Meta timeout、stop/drain。

建议提交：`feat(cache-manager): aggregate cache access reports`

### T18：实现 Storage 本地访问描述更新

涉及文件：

- cache read path in `src/storage/store/`
- CacheChunkDescriptor store
- 新增本地 access coalescer
- `tests/storage/cache/TestLocalCacheAccess.cc`

实施：

- cache generation read 成功后更新 descriptor lastAccessAt。
- 每块按 `local_access_persist_interval` 合并落盘。
- `max(previous, now)` 防止时间回拨。
- 普通 3FS read 不进入该路径。

测试：高频合并、重启持久化、时间回拨、旧 generation read、普通 chain 无变化。

建议提交：`feat(storage): track local cache recency`

M2 退出条件：READY placement 全链路一致；Client/Manager/Metadata 和 Storage 两套近似热度链路可独立退化。

## 8. M3：可插拔策略与正常驱逐

### T19：实现 Eviction Policy 接口、工厂和 LRU

涉及文件：

- 新增 `src/cache_manager/eviction/{EvictionPolicy,LRUEvictionPolicy,EvictionPolicyFactory}.*`
- 新增 Storage 本地 policy 对应接口/工厂
- `tests/cache_manager/TestEvictionPolicy.cc`
- `tests/storage/cache/TestLocalEvictionPolicy.cc`

实施：

- Manager policy 输入 immutable candidates 与 per-disk deficits，只返回 candidate indexes。
- Controller validation helper 检查范围、唯一性、硬保护期、batch 上限和每盘 footprint。
- LRU 以 lastAccessAt/readyAt 从旧到新选取。
- Storage local policy 独立接口，默认 LRU；配置未知策略启动失败。

测试：多盘 deficit、相同时间稳定性、保护期、恶意重复/越界 index、Fake policy、未知配置。

建议提交：`feat(cache): add pluggable eviction policies`

### T20：实现 Metadata EVICTING 状态与重放身份

涉及文件：

- `src/meta/store/cache/CacheBlockRecord.*`
- 新增 `BeginEvictCacheBlocks`、`ListEvictingCacheBlocks` ops
- Meta service/client
- Read Plan 与 BeginClean
- `tests/meta/cache/TestCacheEviction.cc`

实施：

- READY + expected ReadyIdentity 原子进入 EVICTING。
- 只复制 READY PlacementIdentity，生成单调 evictionEpoch 和稳定 retireOperationId。
- 重复 BeginEvict 同 identity 返回原操作；epoch 溢出 fail closed。
- EVICTING 保留 READY identity、placement 和 COMMITTED 统计，但 Read Plan 不返回 hit。
- BeginClean/BeginEvict 互斥；稳定分页列出 EVICTING。

测试：CAS race、重复、旧 READY、Read Plan fallback、epoch overflow、BeginClean 竞争、分页重放。

建议提交：`feat(meta): persist cache eviction operations`

### T21：实现全局候选枚举与物理 footprint 映射

涉及文件：

- 新增 `src/cache_manager/eviction/EvictionCandidateSource.*`
- Meta list/status client
- PhysicalTopology
- `tests/cache_manager/TestEvictionCandidates.cc`

实施：

- 分页读取 READY/COMMITTED block，使用持久化 placement 而非当前 route 选择副本。
- 把每个 replica 的真实 footprint 聚合为 per-disk map。
- 只保留触及受压磁盘的候选；新 READY 保护期是 controller 硬约束。
- placement 无法解析到原磁盘时 fail closed 并报警，不猜新 placement。

测试：路由变化、旧 replica ACTIVE、同盘双副本、未知原磁盘、多盘候选。

建议提交：`feat(cache-manager): enumerate physical eviction candidates`

### T22：实现正常 high/low 水位驱逐控制器

涉及文件：

- 新增 `src/cache_manager/eviction/EvictionController.*`
- CacheManagerOperator background runners
- Config
- `tests/cache_manager/TestEvictionController.cc`

实施：

- 任一磁盘达到 normal high 时暂停涉及 chain 的准入。
- 计算每盘回到 normal low 的 bytes deficit，调用 policy。
- mutation 前重新验证 policy indexes、READY identity、保护期和 footprint。
- 批量 BeginEvict；CAS conflict 跳过；合法操作交给后续 retire executor。
- fresh snapshot 低于 low 才恢复准入；候选不足保持暂停并报警。

测试：迟滞、多盘压力、无关 chain、策略非法输出、候选不足、控制器重启幂等。

建议提交：`feat(cache-manager): evict cache at physical watermarks`

M3 退出条件：正常驱逐可把受压磁盘从 high 降到 low，policy 可替换，路由变化不会删除错误副本。

## 9. M4：可靠事件、复制退役与本地安全兜底

### T23：实现 Storage operation journal 与 delivery outbox

涉及文件：

- 新增 `src/storage/cache/event/{CacheEventJournal,CacheEventOutbox}.*`
- Storage 本地持久化 schema/prefix
- `tests/storage/cache/TestCacheEventJournal.cc`

实施：

- PREPARED operation 只持久化 operationId 与删除意图，不分配 sequence。
- 确认删除后，在本地原子事务中分配下一个 sequence、创建 DELIVERABLE、切状态。
- sourceId 持久化；ACK 后才回收 delivery entry。
- 启动恢复 PREPARED/DELIVERABLE；journal 有界且使用保留空间。
- journal 不可写时禁止新 Phase-2 cache delete/write，不能静默丢事件。

测试：六个本地崩溃点、重启、重复 ACK、compaction、journal full；卡住 A 不阻塞完成的 B/C sequence。

建议提交：`feat(storage): persist cache deletion events`

### T24：实现 Metadata Event 消费、ACK 与 dead letter

涉及文件：

- 新增 Metadata cache event store/op
- `src/meta/service/MetaOperator.*`
- Storage event reporter client
- `tests/meta/cache/TestCacheStorageEvents.cc`

实施：

- 同事务完成 source sequence 校验、状态效果、dead letter 和 ACK cursor。
- duplicate 返回持久化 ACK；gap 不越过。
- DELETED 必须匹配 key、generation、完整 placement、evictionEpoch 和 logical retireOperationId。
- 实现 READY/EVICTING/CLEANING/LOADING/QUEUED/INVALID/FAILED/NONE × 两类事件矩阵。
- stale/future/语义异常 ACK 并 dead-letter/报警，不永久阻塞 source。
- Phase-1 CLEANING 保持原 worker 完成所有权。

测试：完整矩阵、重复/乱序、旧/未来 generation、事务 commit 后 ACK 丢失、exact-once 统计释放。

建议提交：`feat(meta): consume cache storage events idempotently`

### T25：实现 Target generation-retire durable acknowledgement

涉及文件：

- `src/storage/store/{ChunkEngine,ChunkReplica}.*`
- cache retire target RPC
- `tests/storage/cache/TestReplicaRetireOperation.cc`

实施：

- Target 以 stable operationId 执行 generation-fenced retire。
- 只有 tombstone durable 且 query 证明 generation 不可读/不可 replace 才返回 durable-retired。
- same operation 重试幂等；旧/new generation 结果明确。
- 该 target ACK 不等于 logical DELETED。

测试：tombstone 前后崩溃、重复、NotFound、generation advanced、延迟旧 write。

建议提交：`feat(storage): acknowledge durable cache replica retirement`

### T26：实现 coordinated RetireOperation

涉及文件：

- 新增 `src/storage/cache/retire/{RetireOperationStore,RetireCoordinator}.*`
- chain-level retire RPC/client
- `tests/storage/cache/TestRetireCoordinator.cc`

实施：

- coordinatorTargetId 在任何副本删除前持久化完整 RetireOperation。
- expected replica set 只取 READY placement，不随 routing 更新。
- 收集每个 Target 的 durable-retired；部分成功保持 PREPARED 并重试。
- 全部 recorded replicas 完成后才原子创建 logical DELETED DELIVERABLE。
- coordinator 重启恢复；无法联系旧 replica 时保持 PREPARED/EVICTING。

测试：每个 replica 前后崩溃、部分超时、协调者崩溃、route change、旧 replica ACTIVE 禁止 DELETED。

建议提交：`feat(storage): coordinate replicated cache retirement`

### T27：实现本地安全驱逐与 EVICTING 执行恢复

涉及文件：

- 新增 `src/storage/cache/eviction/LocalSafetyEvictor.*`
- 新增 `src/cache_manager/eviction/EvictingWorker.*`
- Storage/Cache Manager background runner
- `tests/storage/cache/TestLocalSafetyEvictor.cc`
- `tests/cache_manager/TestEvictingWorker.cc`

实施：

- Storage 达 local safety high 时仅枚举 CACHE_ONLY descriptor，通过 local policy 选本地 chunk。
- 排除 executing/不稳定 generation；PREPARE event 后删除，降到 local safety low。
- EMERGENCY_EVICTED 携带 descriptor placement 和本地 operationId。
- Metadata READY→EVICTING 时生成新的 logical retire operationId。
- Manager 周期扫描 EVICTING，按持久化 operation 调 coordinated retire；重启从头幂等重放。
- CLEANING event 只 ACK，不抢完成所有权。

测试：本地迟滞、LRU、Manager/Meta down、事件延迟、CLEANING race、重启恢复、最终 NONE。

建议提交：`feat(cache): complete emergency eviction convergence`

M4 退出条件：任一 recorded replica 未 durable-retired 时不可能释放 Metadata；事件至少一次且无 sequence 队头阻塞。

## 10. M5：配置、状态、升级与运维

### T28：补齐配置、状态、指标与告警

涉及文件：

- Cache Manager/Storage Config
- `GetCacheStatus` 与 Admin CLI
- cache metrics/structured logs
- 对应 config/status tests

实施：

- 加入 admission、normal/local 水位、snapshot、permit、policy、event journal 配置和关系校验。
- status 展示 disk role/capacity/used/allocatable/reserved、snapshot age、permit epoch、暂停原因、policy、
  READY/EVICTING、event backlog/ACK/dead letter。
- 实现设计文档列出的 admission/space/permit/eviction/event 指标。
- 日志包含 block、generation、placement、epoch、operation、source/sequence，不泄露凭据。
- 未知 policy、水位不一致、journal 不可写启动失败或 fail closed。

建议提交：`feat(cache): observe phase two capacity control`

### T29：实现升级排空、enable/rollback 前置检查与运行手册

涉及文件：

- Admin cache commands
- Mgmtd capability/enable state
- 新增 `docs/dev/cache-phase-2-runbook.md`
- upgrade tests

实施：

- 命令化：停止 Phase-1 admission、fenced CLEAN 全部记录、验证 Metadata zero/nonterminal zero。
- 查询 Storage，验证无 ACTIVE cache generation。
- inventory-check disk role、stable disk ID、permit store、event journal、routing 和水位。
- 所有组件 capability 就绪后才原子 enable Phase 2。
- rollback 先 drain permit/EVICTING/event，再 CLEAN READY，禁止 descriptor 数据暴露给 Phase-1 binary。
- 记录不可自动恢复的 LOADING/cross-version 情况和人工处置。

测试：带现有 Phase-1 READY 的升级、旧 Manager 写入拒绝、半升级、enable 重试、rollback event backlog。

建议提交：`feat(admin): gate phase two cache rollout`

M5 退出条件：管理员可判断为何停止准入，可安全升级/回滚，Phase-1 旧数据不会进入新 descriptor/event 语义。

## 11. M6：完整闭环与故障验收

### T30：完成集成、故障注入与第二阶段交付

涉及文件：

- `tests/cache/integration/`
- `tests/cache_manager/`
- `tests/meta/cache/`
- `tests/storage/cache/`
- 新增 `docs/dev/cache-phase-2-acceptance.md`

实施与验收：

1. MinIO：第一次 miss 不缓存，第二次 admit，READY hit，上报热度，正常 eviction，重新回源。
2. 物理容量：同盘双副本、短块、预分配 engine、并发 write、Manager crash with inflight write。
3. 水位：normal high/low 与 local safety high/low 均验证迟滞和 chain 隔离。
4. 策略：默认 LRU、Fake policy、恶意输出、配置切换。
5. Placement：READY 后 route change，旧 replica ACTIVE 时绝不 DELETED/释放。
6. Permit：prepare/rollback/renew/expiry/consume、enqueue-before-schedule 崩溃、恢复 CAS 竞争。
7. Retire：每个 replica durable ACK 前后、coordinator crash、partial replica timeout。
8. Event：PREPARED/DELIVERABLE/report/Meta commit/ACK/reclaim 六个崩溃窗口。
9. Event ordering：永久卡住 operation A 不影响已完成 B/C 连续上报。
10. 状态矩阵：BeginEvict/BeginClean/local safety/LOADING/FAILED/NONE 的重复与乱序事件。
11. Manager restart：恢复 QUEUED permit 和 EVICTING；明确不承诺完整 LOADING recovery。
12. 前台读：所有容量、Manager、event 故障下仍安全回源且不返回部分成功。
13. 专用磁盘：所有普通 write/truncate/remove 和 mixed routing 均被拒绝。
14. 全量回归：Phase-1 cache、Metadata、Storage、Mgmtd、Client、analytics、MinIO 不回归。

故障断言：

- outbox PREPARED 失败时物理 chunk 不变；
- 任一 recorded replica ACTIVE 时 Metadata 和逻辑统计不释放；
- logical release 后所有 recorded replicas 均 generation-retired；
- 重复事件不会重复释放统计；
- Access Report 丢失只影响近似策略，不影响正确性；
- Phase-2 disabled 时 Phase-1 原生路径行为不变。

最终验证：

```bash
cmake --build build -j 32
ctest --test-dir build --output-on-failure
cmake --build build --target check-format
```

环境依赖失败必须与功能失败分开记录，不得把 RDMA、io_uring 或既有 retry 基线问题误报为 Phase-2 回归。

建议提交：`test(cache): complete phase two capacity acceptance`

## 12. 任务状态表

| 任务 | 里程碑 | 状态 | 主要交付 |
| --- | --- | --- | --- |
| T0 | M0 | pending | 基线与测试矩阵 |
| T1 | M0 | pending | 公共身份与协议版本 |
| T2 | M0 | pending | Storage 磁盘角色 |
| T3 | M0 | pending | Mgmtd 角色隔离 |
| T4 | M0 | pending | Phase-2 wire contract |
| T5 | M1 | pending | 物理容量与 footprint |
| T6 | M1 | pending | CacheSpaceGate |
| T7 | M1 | pending | 多副本 permit coordinator |
| T8 | M1 | pending | Metadata Permit/Placement |
| T9 | M1 | pending | 逻辑容量只读统计 |
| T10 | M1 | pending | SecondMiss AdmissionPolicy |
| T11 | M1 | pending | Manager 物理拓扑与快照 |
| T12 | M1 | pending | EnsureCached 物理准入 |
| T13 | M2 | pending | Loader Placement/Descriptor |
| T14 | M2 | pending | Permit 恢复与取消 |
| T15 | M2 | pending | Client Access Reporter |
| T16 | M2 | pending | Metadata 访问摘要 |
| T17 | M2 | pending | Manager Access 聚合 |
| T18 | M2 | pending | Storage 本地热度 |
| T19 | M3 | pending | 可插拔 EvictionPolicy/LRU |
| T20 | M3 | pending | Metadata EVICTING |
| T21 | M3 | pending | 全局候选枚举 |
| T22 | M3 | pending | 正常水位驱逐控制器 |
| T23 | M4 | pending | Event Journal/Outbox |
| T24 | M4 | pending | Metadata Event Consumer |
| T25 | M4 | pending | Replica durable retire |
| T26 | M4 | pending | Coordinated RetireOperation |
| T27 | M4 | pending | 本地安全驱逐与恢复 |
| T28 | M5 | pending | 配置、状态与指标 |
| T29 | M5 | pending | 升级、回滚与运行手册 |
| T30 | M6 | pending | 集成与故障验收 |

## 13. 开始实施前的检查

开始 T0 前确认：

- 最终设计文档 commit `e8f9d95` 已在当前分支；
- 用户工作区中的既有删除、子模块修改和 build 目录不纳入任务提交；
- Phase 2 feature 默认关闭；
- 测试环境可区分功能失败与 RDMA/io_uring/FDB/MinIO 环境失败；
- 每个任务完成后及时更新本计划状态表和 acceptance evidence。
