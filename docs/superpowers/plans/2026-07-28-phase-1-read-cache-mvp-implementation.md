# 3FS Cache 第一阶段只读缓存 MVP 实施计划

## 1. 目的与使用方式

本计划把已批准的[第一阶段设计](../specs/2026-07-27-phase-1-read-cache-mvp-design.md)拆成可独立实现、测试和提交的小任务。实施顺序遵循纵向闭环 M0–M5，但优先落下跨模块安全地基：fail-closed OriginFile、容量单一 charge、单调 cacheGeneration、Storage tombstone 和 OriginFile 专用 GC 生命周期。

每个任务完成时应：

1. 只修改列出的职责范围，避免顺手重构；
2. 先补失败测试，再实现，再运行窄测试；
3. 运行 `git diff --check`，检查没有混入用户改动；
4. 使用任务建议的独立提交主题；
5. 里程碑结束后再运行格式检查和组合测试。

性能 benchmark 在本阶段只记录基线，不阻塞任务或里程碑。

## 2. 已核对的仓库事实

- `InodeData::type` 当前是 `std::variant<File, Directory, Symlink>`，大量代码直接使用 `isFile()/asFile()`。
- Metadata RPC 集中在 `src/fbs/meta/Service.h`、`MetaOperator` 和 `MetaSerdeService`。
- Metadata operation 使用 `MetaStore + OperationDriver + FDB transaction` 模式。
- GC 当前把 `InodeType::File` 交给 `FileHelper::remove()` 直接删除 Storage chunk，OriginFile 必须绕开此路径。
- Storage 已有 batch read/write/remove 和 `ChunkMetadata`，但没有 cacheGeneration、generation-fenced replace 或 tombstone。
- FUSE 主 read 路径是 `FuseOps.cc → PioV → StorageClient::batchRead()`。
- CMake 的 `target_add_lib`、`target_add_test` 会递归 glob 当前目录源码；新增顶层模块仍需显式 `add_subdirectory()`。
- 仓库当前没有 AWS SDK 或其他 S3 client 依赖。

## 3. 构建与提交策略

### 3.1 S3 依赖

采用 AWS SDK for C++ S3 client，同时支持 AWS S3 和 MinIO endpoint override。新增 `HF3FS_ENABLE_CACHE` CMake option；开启时通过 `find_package(AWSSDK REQUIRED COMPONENTS s3)` 链接，不把 SDK 源码直接复制进仓库。未开启时，原有 3FS target 仍可构建，但 OriginFile import/read feature gate 保持关闭。

实现提交必须记录 CI/开发镜像中使用的 SDK 版本；若最终选择 vendored/submodule，应另开依赖审计变更，不与业务代码混在同一提交。

### 3.2 建议提交序列

每个以下任务对应一个聚焦提交。若单任务同时修改协议和实现且 diff 过大，可拆成“协议/测试”和“实现”两个连续提交，但不得把后续里程碑功能提前混入。

### 3.3 基线命令

如果已有 `build/`：

```bash
cmake --build build --target test_meta test_storage test_client -j 32
ctest --test-dir build -R '^(test_meta|test_storage|test_client)$' --output-on-failure
```

若需重新配置，沿用仓库指南中的 clang-14、RelWithDebInfo 和显式 SHUFFLE_METHOD；开启缓存功能时追加 `-DHF3FS_ENABLE_CACHE=ON`。

## 4. 依赖图

```text
T0 baseline
 ├─ T1 common cache types/errors
 ├─ T2 OriginFile schema + file-like helpers
 │   ├─ T3 fail-closed/read-only/GC audit
 │   └─ T8 import/refresh namespace
 ├─ T4 CACHE_DATA chain table + feature gate
 ├─ T5 cache protocols and stubs
 │   ├─ T6 Metadata records/accounting
 │   │   ├─ T9 Read Plan
 │   │   ├─ T10 state transitions
 │   │   └─ T13 cleanup job/store
 │   └─ T11 Storage generation protocol
 │       └─ T12 Storage fenced implementation
 ├─ T7 ObjectStore/S3 adapter
 │   └─ T15 Loader
 └─ T14 Cache Manager skeleton
     ├─ T15 admission/scheduler/loader
     ├─ T16 cleanup workflow
     └─ T17 client hit/miss pipeline
          └─ T18 FUSE/Native integration

T19 Admin + T20 metrics + T21 MinIO + T22 AWS qualification + T23 final regression
```

## 5. M0：协议、安全地基与测试骨架

### T0：冻结基线并建立缓存测试入口

涉及文件：

- `tests/CMakeLists.txt`
- 新增 `tests/cache/CMakeLists.txt`
- 新增 `tests/cache/TestPlaceholder.cc`
- 新增 `tests/cache_manager/CMakeLists.txt`
- 新增 `tests/cache_manager/TestPlaceholder.cc`
- 新增 `tests/meta/cache/`、`tests/client/cache/`、`tests/storage/cache/` 目录

实施：

- 记录当前 `test_meta`、`test_storage`、`test_client` 结果。
- 新增 `test_cache` 和 `test_cache_manager` 空测试 target，确保 ObjectStore/集成层和服务代码有独立测试入口。
- 不移动现有测试；子目录测试仍由各子系统递归 glob 收集。

验证：

```bash
cmake --build build --target test_cache test_cache_manager test_meta test_storage test_client -j 32
ctest --test-dir build -R '^(test_cache|test_cache_manager|test_meta|test_storage|test_client)$' --output-on-failure
```

建议提交：`test(cache): establish phase one test targets`

### T1：定义跨模块 Cache 基础类型与错误码

涉及文件：

- 新增 `src/fbs/cache/Common.h`
- 修改 `src/fbs/CMakeLists.txt`
- 新增 `src/fbs/cache/CMakeLists.txt`
- 修改公共 Status/Error code 注册文件
- 新增 `tests/common/cache/TestCacheTypes.cc`

实施：

- 定义 OriginId、ObjectRef、VersionSelector、ImmutableObjectIdentity、ByteRange。
- 定义 CacheBlockKey、CacheBlockState、ChargeKind、CacheGeneration、ReadyIdentity、CleanupEpoch。
- CacheGeneration 使用同一 CacheBlockKey 内严格单调的 `uint64_t` counter；序列化格式固定并有边界测试。
- 定义稳定错误：FeatureDisabled、UpgradeRequired、VersionMismatch、StaleGeneration、CapacityExceeded、StateConflict、ReadOnlyOriginFile。
- 类型中不得出现 AWS SDK、endpoint 或 credential 字段。

测试：

- serde round-trip、完整对象身份比较、same ETag/different key、generation 排序、溢出/无效值。

验证：

```bash
cmake --build build --target common cache-fbs test_common -j 32
ctest --test-dir build -R '^test_common$' --output-on-failure
```

建议提交：`feat(cache): define shared cache types and errors`

### T2：加入 fail-closed OriginFile inode variant

涉及文件：

- `src/fbs/meta/Common.h`
- `src/fbs/meta/Schema.h`
- `src/fbs/meta/Schema.cc`
- `src/fbs/meta/Utils.h`
- `src/meta/store/Inode.h`
- `tests/meta/store/TestInode.cc` 或新增 `tests/meta/cache/TestOriginFileSchema.cc`

实施：

- 新增独立 `OriginFile` variant 和 `InodeType::OriginFile`，不在 `File` 尾部追加字段。
- OriginFile 保存 length、Layout、不可变对象身份、superseded/cacheAdmissionDisabled 和 cleanupJobId。
- 抽取 `isRegularFileLike()`、`fileLength()`、`fileLayout()`、`getChunkId()`、`getChainId()` 等只读 helper；现有 File 行为不变。
- `DirEntryData::valid()`、stat/type formatter 和 serde 覆盖新 variant。
- 未知 variant decode 必须返回错误，不能 fallback 为 File。

测试：

- OriginFile serde、helper 与 ChunkId/ChainId 计算。
- 模拟旧 variant reader，确认未知 OriginFile fail-closed。
- 原 File 序列化字节和行为不回归。

验证：

```bash
cmake --build build --target meta-fbs test_meta -j 32
ctest --test-dir build -R '^test_meta$' --output-on-failure
```

建议提交：`feat(meta): add fail-closed origin file inode`

### T3：审计 file-like 调用并建立只读与 GC 防线

涉及文件：

- `src/fbs/meta/FileOperation.*`
- `src/meta/store/ops/Open.cc`
- `src/meta/store/ops/Remove.cc`
- `src/meta/store/ops/Rename.cc`
- `src/meta/store/ops/SetAttr.*`
- `src/meta/components/FileHelper.*`
- `src/meta/components/GcManager.*`
- `src/fuse/FuseOps.cc`
- `src/fuse/FuseClients.*`
- `src/fuse/PioV.*`
- `tests/meta/store/ops/TestOpen.cc`
- `tests/meta/store/ops/TestRemove.cc`
- `tests/meta/store/ops/TestRename.cc`

实施：

- 用 `rg 'isFile|asFile' src/fbs/meta src/meta src/fuse src/lib` 建立逐处清单。
- 只读/stat/read 所需位置使用 file-like helper；写入位置继续只接受 File。
- OriginFile 对 O_WRONLY/O_RDWR/O_TRUNC、sync length、truncate、set length、write、fallocate/hole-punch 返回统一只读错误；FUSE 映射 EROFS。
- 第一阶段拒绝普通 unlink 和任何涉及 OriginFile 的 rename/overwrite。
- GcManager 遇到 OriginFile 时只检查 cleanupJob COMPLETE 和无 session；不得调用 FileHelper::removeChunks。

测试：

- 所有写/open flag、unlink/rename 拒绝。
- OriginFile stat/list/open(O_RDONLY) 正常。
- GC 在 cleanup 未完成时 Busy，完成后只删 inode/GC entry。

验证：

```bash
cmake --build build --target test_meta hf3fs_fuse -j 32
ctest --test-dir build -R '^test_meta$' --output-on-failure
```

建议提交：`feat(meta): enforce origin file lifecycle boundaries`

### T4：CACHE_DATA Chain Table 与能力门禁

涉及文件：

- `src/fbs/mgmtd/ChainTable.h`
- `src/fbs/mgmtd/MgmtdTypes.h`
- `src/fbs/mgmtd/RoutingInfo.h`
- `src/mgmtd/store/MgmtdStore.*`
- `src/mgmtd/ops/SetChainTableOperation.*`
- `src/client/cli/admin/{UploadChainTable,ListChainTables,DumpChainTable}.cc`
- Metadata/Client/FUSE heartbeat capability 字段
- `tests/mgmtd/` 和 `tests/meta/cache/TestCacheFeatureGate.cc`

实施：

- 增加 ChainTableRole、logicalCapacity、checksumType 和 cache schema/protocol capability。
- OriginFile layout 只接受 CACHE_DATA；普通 File 拒绝 CACHE_DATA。
- mgmtd 从活跃 Metadata writer heartbeat 聚合能力；gate 未满足时 Import/Refresh 返回 FeatureDisabled。
- Client/FUSE/Admin 请求携带 cacheProtocolVersion；不兼容返回 UpgradeRequired。
- 激活后配置禁止回退到不支持 OriginFile 的 schema version。
- 第一阶段 `cache-admin` 映射为已认证 `UserAttr.admin == true` 或 root；Metadata 通过现有 UserStore 加载属性，不能信任 UserInfo 自报角色。Cache Manager service identity 使用单独 token/ACL，不复用普通用户凭证。

测试：

- ChainTable 属性持久化、升级默认值、role 校验。
- 缺少一个 Metadata capability 时 gate 关闭；全部满足后开启。
- 旧 Client 请求被明确拒绝。

验证：

```bash
cmake --build build --target test_mgmtd test_meta admin_cli -j 32
ctest --test-dir build -R '^(test_mgmtd|test_meta)$' --output-on-failure
```

建议提交：`feat(mgmtd): add cache chain role and capability gate`

### T5：固定 Metadata、Storage、Cache Manager wire contract

涉及文件：

- `src/fbs/meta/Service.h`
- `src/meta/service/MetaSerdeService.h`
- `src/client/meta/MetaClient.*`
- `src/fbs/storage/Common.h`
- `src/fbs/storage/Service.h`
- 新增 `src/fbs/cache_manager/{Common,Service}.h`
- 新增 `src/fbs/cache_manager/CMakeLists.txt`
- 新增 `src/stubs/cache_manager/`
- `src/stubs/CMakeLists.txt`

实施：

- 按设计文档 §4.10 定义 bounded requests、逐项结果和稳定 method ID。
- Metadata：Import/BatchImport/Refresh/GetReadPlan/Enqueue/Acquire/Commit/Fail/BeginClean/FinishClean/Status/List。
- Storage：replaceCacheChunkIfNewer、retireCacheChunkGeneration、query cache generation。
- Cache Manager：EnsureCached、ReportInvalid、AdminCleanup、GetStatus。
- 所有批量列表硬限制 1000，响应保持输入顺序。
- Commit/Fail 均携带 loaderId/loadEpoch；ReportInvalid 携带 ReadyIdentity 和 observed generation。

测试：

- 所有协议 serde、valid()、请求上限、method ID 唯一性。
- 不连接服务的 stub 编译测试。

验证：

```bash
cmake --build build --target meta-fbs storage-fbs cache-manager-fbs meta-client -j 32
```

建议提交：`feat(cache): define phase one service contracts`

### T6：实现 CacheBlockStore、单一 charge 与 generation 分配

涉及文件：

- `src/common/kv/KeyPrefix-def.h`
- 新增 `src/meta/store/cache/{CacheBlockRecord,CacheBlockStore,CacheCapacityStore}.*`
- `src/meta/store/MetaStore.*`
- `tests/meta/cache/TestCacheBlockStore.cc`
- `tests/meta/cache/TestCacheCapacity.cc`

实施：

- 新增不冲突的 4-byte KeyPrefix：block、capacity、refresh request、cleanup job。
- Record 保存 state、chain、length、loader/epoch、cacheGeneration、ReadyIdentity、chargeKind/chargedBytes、cleanupEpoch/terminalState。
- NONE 不落 record；FAILED 为无 charge terminal record。
- Enqueue 在同一 FDB transaction 创建 RESERVED charge并更新全局 used；超限逐项拒绝。
- Acquire 在同一事务中读取并递增 `/cache/generation/{inode}/{block}`；counter 独立于 CacheBlockRecord，cleanup/record 删除后保留，溢出时拒绝 Acquire。
- Commit 只把 RESERVED 改为 COMMITTED；进入 CLEANING 继承同一 charge；FinishClean 释放一次。

测试：

- 每个 source state → CLEANING 的 charge 不重复。
- 100 个并发 Enqueue 不超过 logicalCapacity。
- Fail、FinishClean、REENQUEUE 的计数一致性。
- FDB transaction 重试不重复收费；同一 block 的成功 Acquire generation 严格递增且不因 cleanup 重置。

验证：

```bash
cmake --build build --target test_meta -j 32
ctest --test-dir build -R '^test_meta$' --output-on-failure
```

建议提交：`feat(meta): persist cache blocks and atomic capacity charges`

## 6. M1：Origin namespace 与 miss-only 读取

### T7：引入 AWS SDK 并实现 ObjectStore/S3 Adapter

涉及文件：

- `CMakeLists.txt`
- 新增 `cmake/AwsSdk.cmake`
- `src/CMakeLists.txt`
- 新增 `src/cache/origin/`
- 新增 `src/cache/origin/s3/`
- 新增 `src/cache/CMakeLists.txt`
- 新增 `tests/cache/origin/TestS3ObjectStore.cc`

实施：

- 增加 `HF3FS_ENABLE_CACHE` 和 AWSSDK S3 依赖探测。
- 实现 ObjectStore interface、factory、credential provider 和专用 IO executor。
- S3 adapter 支持 endpoint override、path-style MinIO、AWS region/TLS。
- HEAD 规范化 VersionId/强 ETag；Range GET 使用 VersionId 或 If-Match。
- checked range arithmetic；非零 range 必须验证 206、Content-Range 和 body length。
- 仅 retry timeout、429、可恢复 5xx，并受总 timeout、次数、并发和 inflight bytes 限制。
- 凭证只存在本地配置/provider，日志脱敏。

测试：

- 使用 fake SDK executor 测错误映射、重试预算、版本选择和严格响应校验。
- MinIO 网络测试留到 T21。

验证：

```bash
cmake --build build --target cache-origin test_cache -j 32
ctest --test-dir build -R '^test_cache$' --output-on-failure
```

建议提交：`feat(origin): add versioned S3 object store adapter`

### T8：实现 Import、BatchImport、Refresh 与 cleanupJob 持久化

涉及文件：

- 新增 `src/meta/store/ops/{ImportOriginFile,RefreshOriginFile}.*`
- 新增 `src/meta/components/OriginNamespaceManager.*`
- `src/meta/store/MetaStore.*`
- `src/meta/service/MetaOperator.*`
- `src/meta/service/MetaSerdeService.h`
- `src/client/meta/MetaClient.*`
- `tests/meta/cache/TestOriginNamespace.cc`

实施：

- Import 校验 cache-admin、feature gate、完整对象身份、size/block count 和 CACHE_DATA layout。
- BatchImport 最多 1000，逐项事务/结果；重复 path 按设计语义处理。
- Refresh 单事务完成 expectedInode CAS、旧 inode superseded、dentry replacement、requestId 幂等结果和 cleanupJob 创建。
- cleanupJob 保存 cursor、总 block range、批次状态和精确 COMPLETE predicate。
- 同 requestId 在 commit-to-attach 崩溃后返回原 newInode/cleanupJobId。

测试：

- 单/批量导入、幂等、部分失败、重复 path、权限。
- same ETag/different key 和 same VersionId string/different bucket。
- Refresh 并发、同 requestId 重试、不同 requestId stale expectedInode。
- 大于 1000 block cleanup cursor 持久化。

验证：

```bash
cmake --build build --target test_meta meta-client -j 32
ctest --test-dir build -R '^test_meta$' --output-on-failure
```

建议提交：`feat(meta): import and refresh origin namespace`

### T9：建立共享 CacheReadPipeline skeleton 和 miss-only Native 读取

涉及文件：

- 新增 `src/client/cache/{CacheReadPipeline,OriginMissReader,BufferAssembler,OriginRangePlanner,LocalMissSingleflight}.*`
- 新增 `src/client/cache/CMakeLists.txt`
- `src/client/CMakeLists.txt`
- `src/lib/api/CMakeLists.txt`
- `src/lib/api/UsrbIo.cc`
- `tests/client/cache/TestOriginMissReader.cc`
- `tests/client/cache/TestCacheReadPipelineMiss.cc`

实施：

- pipeline 接受 inode、open session、offset、length、output。
- M1 暂把所有 OriginFile block 当 miss；普通 File 不进入 pipeline。
- 合并连续范围，最大 256 MiB；受并发/inflight 配置限制。
- singleflight key 使用完整不可变对象身份和 aligned range。
- BufferAssembler 正确处理非对齐、EOF、零长度和分批结果。
- Native API 所有 OriginFile 读取走此 pipeline；不得创建临时第二套 reader。

测试：

- 0/1/block±1、跨 block、EOF、乱序完成、单段失败。
- singleflight 成功、失败传播和 Future 清理。
- VersionMismatch 不返回部分数据。

验证：

```bash
cmake --build build --target test_client hf3fs_api -j 32
ctest --test-dir build -R '^test_client$' --output-on-failure
```

建议提交：`feat(client): add miss-only origin read pipeline`

## 7. M2：Read Plan、状态机和 Storage fencing

### T10：实现 GetFileReadPlan

涉及文件：

- 新增 `src/meta/store/ops/GetFileReadPlan.*`
- `src/meta/store/cache/CacheBlockStore.*`
- `src/meta/store/MetaStore.*`
- `src/meta/service/MetaOperator.*`
- `src/meta/service/MetaSerdeService.h`
- `src/client/meta/MetaClient.*`
- `tests/meta/cache/TestGetFileReadPlan.cc`

实施：

- 校验 UserInfo、open session、read permission、OriginFile 和完整对象身份。
- checked offset/length/EOF，最多 1000 blocks。
- 批量 snapshot 读 CacheBlockRecord；NONE 不写 FDB。
- 使用 file-like helper 计算 ChunkId/ChainId；返回 ReadyIdentity/cacheGeneration。
- Client 大读取分批，每批 inode/object identity 不一致时整体重新规划。

测试：

- EOF、非对齐、0/1/100/1000/1001 blocks。
- NONE 无写、permission/session 拒绝、superseded old fd 语义。
- 分批 identity 变化触发 replan。

验证：

```bash
cmake --build build --target test_meta test_client -j 32
ctest --test-dir build -R '^(test_meta|test_client)$' --output-on-failure
```

建议提交：`feat(meta): return cache-aware file read plans`

### T11：实现 Metadata Enqueue/Acquire/Commit/Fail 状态转换

涉及文件：

- 新增 `src/meta/store/ops/{EnqueueCacheBlocks,AcquireCacheBlocks,CommitCacheBlocks,FailCacheBlocks}.*`
- `src/meta/store/MetaStore.*`
- `src/meta/service/MetaOperator.*`
- `src/client/meta/MetaClient.*`
- `tests/meta/cache/TestCacheStateMachine.cc`

实施：

- Enqueue：NONE/FAILED→QUEUED，幂等处理已有状态并原子 reserve。
- Acquire：QUEUED 或 expired LOADING→LOADING，递增 loadEpoch、分配 cacheGeneration 和 lease。
- Commit：CAS loaderId/loadEpoch/generation、未 superseded、LOADING；转 READY/COMMITTED。
- Fail：同样 CAS；转 CLEANING、terminalState FAILED，不直接释放 charge。
- 所有批量操作逐项返回；stale Commit/Fail 为 Conflict/no-op。

测试：

- 两 Loader 并发 Acquire 只有一个成功。
- lease reclaim 后旧 Commit 和延迟 Fail 不影响新 epoch/charge。
- refresh superseded 后 Enqueue/Acquire/Commit 拒绝。
- transaction retry 幂等。

验证：

```bash
cmake --build build --target test_meta -j 32
ctest --test-dir build -R '^test_meta$' --output-on-failure
```

建议提交：`feat(meta): enforce fenced cache block transitions`

### T12：实现 Storage generation-fenced replace、read 和 tombstone

涉及文件：

- `src/fbs/storage/Common.h`
- `src/fbs/storage/Service.h`
- `src/storage/service/StorageOperator.*`
- `src/storage/store/{ChunkEngine,ChunkStore,StorageTarget}.*`
- `src/client/storage/{StorageClient,StorageClientImpl,StorageMessenger}.*`
- `tests/storage/cache/TestCacheGeneration.cc`
- `tests/storage/client/` 和 `tests/storage/service/`

实施：

- cache chain chunk metadata 保存 active generation 或 retired tombstone。
- `replaceCacheChunkIfNewer` 实现同 generation/operationId 幂等、更高替换、更低拒绝。
- full replacement 强制 offset 0、exact actualBlockLength、truncate old tail。
- `retireCacheChunkGeneration` 在无数据时也安装 tombstone；GenerationAdvanced 明确返回。
- read/query 返回 generation、length、checksum。
- 只对 CACHE_DATA chain 开启新语义，普通 write/remove 完全不变。
- tombstone 第一阶段不回收，并增加计数指标供后续评估。

测试：

- delayed old write after new READY。
- delayed old write after cleanup/re-enqueue。
- same generation retry、different payload conflict。
- shorter last block 覆盖、ambiguous timeout retry、tombstone NotFound path。
- 普通 chain 行为不变。

验证：

```bash
cmake --build build --target test_storage_store test_storage_service test_storage_client -j 32
ctest --test-dir build -R '^test_storage_(store|service|client)$' --output-on-failure
```

建议提交：`feat(storage): fence cache chunk generations`

### T13：实现 Metadata BeginClean/FinishClean 和 inode cleanup cursor

涉及文件：

- 新增 `src/meta/store/ops/{BeginCleanCacheBlocks,FinishCleanCacheBlocks}.*`
- `src/meta/store/cache/{CacheBlockStore,CleanupJobStore}.*`
- `src/meta/components/GcManager.*`
- `tests/meta/cache/TestCacheCleanup.cc`

实施：

- QUEUED/LOADING/READY/FAILED→CLEANING，递增 loadEpoch、设置 cleanupEpoch/terminalState/deleteGeneration，继承单一 charge。
- FinishClean 要求 Storage tombstone acknowledgement 和相同 cleanupEpoch。
- terminal NONE 删除 record；FAILED 留无 charge record；REENQUEUE 重新申请容量。
- cleanupJob cursor 每批最多 1000，COMPLETE 前 GC 一律 Busy。
- 重复 Begin/Finish/Job advance 幂等。

测试：

- 每种 source state 到 CLEANING 的 chargeKind/chargedBytes。
- stale FinishClean、部分批失败、Manager restart cursor resume。
- refresh with open session、close 后 GC、容量最终释放。

验证：

```bash
cmake --build build --target test_meta -j 32
ctest --test-dir build -R '^test_meta$' --output-on-failure
```

建议提交：`feat(meta): coordinate fenced cache cleanup jobs`

## 8. M3：Cache Manager 与完整缓存读闭环

### T14：建立 Cache Manager 服务、配置和进程生命周期

涉及文件：

- 新增 `src/cache_manager/CMakeLists.txt`
- 新增 `src/cache_manager/cache_manager.cpp`
- 新增 `src/cache_manager/service/{CacheManagerServer,CacheManagerOperator,CacheManagerSerdeService}.*`
- 新增 `src/cache_manager/config/Config.h`
- `src/CMakeLists.txt`
- 新增 `configs/cache_manager_main*.toml`
- `tests/cache_manager/`

实施：

- 复用 `TwoPhaseApplication`、ServerLauncher、mgmtd client、StorageClient 和 MetaClient 模式。
- 注册 Cache Manager serde service；启动/停止 IO executor、scheduler 和 background client。
- 配置全局/单 Origin 并发、inflight bytes、range size、lease、hint timeout 和 OriginId 映射。
- service identity 只具备 cache state RPC 与 CACHE_DATA chain 权限。

测试：

- 配置 serde、启动/停止、重复 stop、依赖启动失败回滚。

验证：

```bash
cmake --build build --target cache_manager_main test_cache_manager -j 32
ctest --test-dir build -R '^test_cache_manager$' --output-on-failure
```

建议提交：`feat(cache-manager): add service skeleton`

### T15：实现 Hint、容量准入、Scheduler 和 Loader

涉及文件：

- 新增 `src/cache_manager/service/EnsureCached.*`
- 新增 `src/cache_manager/scheduler/{HintCoalescer,LoaderScheduler}.*`
- 新增 `src/cache_manager/admission/CapacityGate.*`
- 新增 `src/cache_manager/loader/CacheLoader.*`
- `tests/cache_manager/{TestEnsureCached,TestLoaderScheduler,TestCacheLoader}.cc`

实施：

- EnsureCached 合并 inode/block range；READY/有效 LOADING 返回已有状态。
- QUEUED 必须重新附着当前 scheduler；CLEANING 附着 cleanup workflow。
- NONE/FAILED 逐 block Enqueue；容量拒绝返回 BYPASSED，不影响 foreground read。
- Loader Acquire 后按完整对象身份合并 S3 range，拆 block、算 checksum。
- 调 Storage generation-fenced full replace，验证 result 后逐 block Commit。
- 任一失败调用 fenced Fail；进入 CLEANING 后不自行释放 charge。

测试：

- hint dedup、queue restart reattach、priority FIFO tie-break。
- range 合并/切分、部分 write success、VersionMismatch、superseded inode。
- Loader 在 S3 前、write 后、Commit timeout 的状态。

验证：

```bash
cmake --build build --target test_cache_manager -j 32
ctest --test-dir build -R '^test_cache_manager$' --output-on-failure
```

建议提交：`feat(cache-manager): load admitted blocks into storage`

### T16：实现 ReportInvalid、AdminCleanup 和 Storage tombstone workflow

涉及文件：

- 新增 `src/cache_manager/service/{ReportCacheBlockInvalid,AdminCleanupCacheBlocks}.*`
- 新增 `src/cache_manager/cleanup/CacheCleanupWorker.*`
- `tests/cache_manager/TestCacheCleanupWorker.cc`
- `tests/meta/cache/TestCleanupRaces.cc`
- `tests/storage/cache/TestCacheCleanupRaces.cc`

实施：

- ReportInvalid 校验 user/open session 和 observed ReadyIdentity。
- Manager 等待 BeginClean CAS 成功，再附着 cleanup；CAS stale 为 no-op。
- CleanupWorker 调 retire generation，处理 GenerationAdvanced：query、确认同 cleanupEpoch、更新 deleteGeneration、重试。
- 只有 tombstone ≥ deleteGeneration 且无 active data才 FinishClean。
- AdminCleanup 同 request/job 可重复恢复；Manager 重启依赖 Admin retry 或下一次 Ensure hint 附着。

测试：

- delayed ReportInvalid vs new READY。
- hint-before-invalidation、cleanup-vs-LOADING。
- delayed cleanup after new READY、GenerationAdvanced、partial batch retry。
- Manager 在 CLEANING 中退出后恢复。

验证：

```bash
cmake --build build --target test_cache_manager test_meta test_storage_service -j 32
ctest --test-dir build -R '^(test_cache_manager|test_meta|test_storage_service)$' --output-on-failure
```

建议提交：`feat(cache-manager): clean invalid cache generations safely`

### T17：完成 hit/miss/mixed CacheReadPipeline

涉及文件：

- 新增 `src/client/cache/{ReadPlanner,CacheHitReader,EnsureCachedReporter}.*`
- 修改 `src/client/cache/CacheReadPipeline.*`
- `src/client/meta/MetaClient.*`
- Cache Manager client/stub
- `tests/client/cache/{TestReadPlanner,TestCacheHitReader,TestMixedRead}.cc`

实施：

- 调 GetFileReadPlan，将 READY 与 miss 分类；CLEANING/QUEUED/LOADING/FAILED 均回源。
- READY 完整读 actualBlockLength，同时校验 generation、length、checksum，再切片。
- hit 与连续 miss 并发，BufferAssembler 按 offset 组合。
- NotFound/generation/checksum mismatch 只 fallback 失败 segment，并异步 ReportInvalid + EnsureCached。
- Ensure/Report 使用短 timeout，失败不影响已成功用户读取。
- 全 hit 不访问 S3，也不调用 Cache Manager。

测试：

- 全 hit、全 miss、50% mixed、首尾非对齐、分批 Read Plan。
- physical generation mismatch、NotFound、checksum corruption。
- Cache Manager down 时 cold read 与已有 hit。
- 任一必要 segment 最终失败时不返回部分成功。

验证：

```bash
cmake --build build --target test_client -j 32
ctest --test-dir build -R '^test_client$' --output-on-failure
```

建议提交：`feat(client): read mixed cache and origin ranges`

## 9. M4：FUSE 与 Native API 统一入口

### T18：把 FUSE/PioV 接到共享 CacheReadPipeline

涉及文件：

- `src/fuse/FuseClients.*`
- `src/fuse/FuseConfig.h`
- `src/fuse/FuseOps.cc`
- `src/fuse/PioV.*`
- `src/fuse/CMakeLists.txt`
- `src/lib/api/UsrbIo.cc`
- `tests/lib/` 或新增 FUSE scenario tests

实施：

- FuseClients 构造共享 CacheReadPipeline、ObjectStore 和 Cache Manager client。
- FileHandle 保存 inode snapshot 与 read session；OriginFile read 调 pipeline，File 保持 PioV 原路径。
- 普通 File 不增加 GetReadPlan RPC。
- FUSE 与 Native API 统一错误映射、EOF 和只读 EROFS。
- 不把 pipeline 逻辑复制进 FuseOps。

测试：

- open/stat/read/pread/readdir、并发/random range。
- refresh 前后旧/new fd。
- OriginFile 与 File 同目录；File 路径 RPC/性能行为不变。
- 所有写入口 EROFS。

验证：

```bash
cmake --build build --target hf3fs_fuse_main hf3fs_api test_lib test_client -j 32
ctest --test-dir build -R '^(test_lib|test_client)$' --output-on-failure
```

建议提交：`feat(fuse): route origin files through cache pipeline`

## 10. M5：管理、可观测性与集成验收

### T19：实现第一阶段 Admin CLI

涉及文件：

- 新增 `src/client/cli/admin/CacheImport.*`
- 新增 `CacheRefreshOrigin.*`、`CacheStatus.*`、`CacheListBlocks.*`、`CacheCleanup.*`
- `src/client/cli/admin/registerAdminCommands.cc`
- `src/client/cli/admin/CMakeLists.txt`
- CLI tests

实施：

- `cache-import` 先 HEAD，再发单/批量 Import；输出逐项结果。
- `cache-refresh-origin` 生成稳定 requestId；收到 cleanupJobId 后附着/恢复并展示状态。
- status 展示 used、chargeKind、state counts、queue/inflight、bypass reason。
- list blocks 分页；cleanup 可按 inode/range/job 重试。
- 凭证和签名请求不输出。

验证：

```bash
cmake --build build --target admin_cli -j 32
```

建议提交：`feat(admin): manage origin cache lifecycle`

### T20：补齐基础指标和结构化日志

涉及文件：

- `src/client/cache/`
- `src/meta/store/cache/`
- `src/cache_manager/`
- `src/storage/`

实施：

- Client：hit/miss/origin bytes、Read Plan、Storage/Origin latency、hint/report result。
- Metadata：state transition、charge bytes、epoch conflict、cleanup job state。
- Manager：queue/inflight、admission result、loader/cleanup result。
- Storage：cache generation replace/stale/tombstone counts。
- 所有日志带 inode/block/originId，不含 credential、secret 或签名 URL。

测试：

- 关键路径 recorder 增量和 reason tag；敏感字段日志扫描。

建议提交：`feat(cache): add phase one observability`

### T21：MinIO 自动化集成测试

涉及文件：

- 新增 `tests/cache/integration/`
- `tests/CMakeLists.txt`
- CI 配置/测试说明

实施：

- 测试环境提供隔离 MinIO endpoint；`HF3FS_ENABLE_CACHE_INTEGRATION_TESTS=ON` 时缺失 endpoint 必须失败而非静默 skip。
- 建固定对象：0、1、block-1、block、block+1、多 block。
- 覆盖 Import→cold read→background load→warm hit、mixed、refresh、VersionMismatch、capacity bypass、cleanup。
- 记录 S3 request count，确认 warm all-hit 为 0。

验证：

```bash
ctest --test-dir build -R '^test_cache_minio$' --output-on-failure
```

建议提交：`test(cache): add MinIO read cache integration`

### T22：AWS S3 release-qualification

涉及文件：

- 新增 `tests/cache/integration/TestAwsS3.cc`
- 新增 `docs/dev/cache-aws-qualification.md`
- 可选 CI/manual workflow

实施：

- 凭证、bucket、region 由外部注入；普通 CI 显示明确 SKIPPED。
- release qualification 必须显式启用且保存测试日志/对象清理结果。
- 覆盖 VersionId bucket 和无 versioning 的 If-Match bucket。
- 不把真实 bucket、account、credential 写入仓库。

验证：

```bash
ctest --test-dir build -R '^test_cache_aws_s3$' --output-on-failure
```

建议提交：`test(cache): add AWS S3 qualification suite`

### T23：非阻塞 benchmark、全量回归和第一阶段交付

涉及文件：

- 新增 `benchmarks/cache_bench/CMakeLists.txt`
- 新增 `benchmarks/cache_bench/CacheBench.cc`
- `benchmarks/CMakeLists.txt`
- 第一阶段运行手册/验收记录

实施：

- 场景：cold direct S3、foreground miss、warm hit、mixed、原生 3FS。
- 输出吞吐、P50/P99、S3 request 数、hit ratio；不设通过阈值。
- 完成故障注入：S3 timeout/429/500、Manager stop、Storage stale generation、Commit timeout。
- 汇总未进入第一阶段的 eviction、full reconcile、prefetch、write-through backlog。

最终验证：

```bash
cmake --build build -j 32
ctest --test-dir build --output-on-failure
cmake --build build --target check-format
cmake --build build --target cache_bench -j 32
```

还需分别运行 MinIO 必选测试和 AWS release-qualification，并保存结果。benchmark 数值只记录。

建议提交：`test(cache): complete phase one acceptance coverage`

## 11. 里程碑退出条件

### M0 完成

- OriginFile fail-closed；普通 File 回归通过。
- CACHE_DATA role、feature gate 和 wire contract 固定。
- Metadata charge/generation 基础 record 可原子测试。

### M1 完成

- 显式单/批量导入和幂等 Refresh 可用。
- Native API 可通过共享 pipeline 直接回源。
- OriginFile 所有写和普通 namespace 删除入口 fail-closed。

### M2 完成

- Read Plan、状态转换、容量 charge、Storage generation/tombstone 全部通过竞态测试。
- 旧 Loader write/Commit/Fail 和旧 cleanup 不能破坏新 generation。

### M3 完成

- 第一次 S3、后台填充、第二次 Storage 命中闭环可重复。
- Manager down 不影响 successful origin fallback 或已有 hit。
- READY 异常可 fenced cleanup 并重新加载。

### M4 完成

- FUSE 和 Native API 共用 pipeline，结果一致。
- 普通 3FS File 路径不调用缓存 RPC。

### M5 完成

- MinIO 自动化必选测试通过。
- AWS release-qualification 有外部凭证运行证据。
- Admin、指标、非阻塞 benchmark 和全量回归完成。

## 12. 实施注意事项

- 不要一次性改完所有 `asFile()`；先区分只读 file-like 使用和真正可写 File 使用。
- 不要让 Client 写 cache chunk；只有 Cache Manager Loader 有该权限。
- 不要使用现有无条件 `removeChunks` 清理 cache generation。
- 不要在 Metadata record 完成 CAS 前发布 READY，也不要在 Storage tombstone确认前释放 charge。
- 不要将 endpoint/credential 放入 inode、RPC、日志或测试 fixture。
- 不要把 eviction、Storage Event、full reconcile、prefetch 或 write-through 偷渡进第一阶段。
- 每次协议修改同步更新 MetaClient/StorageClient/stub、valid() 和 serde 测试。
- 工作区现有文档删除和 `docs/dev/` 未跟踪内容属于用户工作，实施提交不得包含。
