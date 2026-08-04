# 3FS Cache 第三阶段数据编排实施计划

## 1. 使用方式

本计划把第三阶段设计拆成 31 个任务（T0..T30）。顺序先冻结兼容协议和持久化不变量，再接 Planner、调度、Ready Ratio、Pin 和运维入口。

每个任务必须补窄测试、运行 `git diff --check`，并且不修改 `third_party/` 或用户现有工作。Phase 3 默认关闭；任何编排失败都不能阻塞前台 Origin fallback。

## 2. 依赖图

```text
M0 基线与协议       T0 -> T1 -> T2 -> T3
M1 持久化地基       T3 -> T4 -> T5 -> T6 -> T7
M2 Planner           T7 -> T8 -> T9 -> T10 -> T11 -> T12 -> T13
M3 Admission/调度    T5/T7 -> T14 -> T15 -> T16 -> T17 -> T18 -> T19
M4 Ready/Cancel      T13/T19 -> T20 -> T21 -> T22
M5 Pin               T6/T20 -> T23 -> T24 -> T25
M6 API 与运维        T19/T22/T25 -> T26 -> T27 -> T28 -> T29
M7 验收              T0..T29 -> T30
```

## 3. M0：基线与协议

### T0：冻结 Phase-3 基线

- 新增 `docs/dev/cache-phase-3-baseline.md`。
- 记录分支、Phase-2 acceptance commit、用户未提交改动和测试环境。
- 固定 cache/meta/storage/cache-manager/admin/MinIO 的过滤组。
- 明确环境失败与功能失败边界。

完成条件：基线命令和结果可复现。

建议提交：`test(cache): record phase three baseline`

### T1：定义 Job、Source、Plan 与 Pin 公共类型

- 修改 `src/fbs/cache_manager/Common.h` 和公共 cache types。
- 增加 JobId、状态枚举、source、spec、plan entry、pin owner/record。
- priority 非负且数值越大越高；ready bps 为 1..10000。
- 所有 `valid()` 覆盖类型相关字段、路径、数量和溢出。

测试：serde round-trip、非法 union 组合、边界、旧 payload 默认值。

建议提交：`feat(cache): define orchestration identities`

### T2：追加 Phase-3 wire contract

- 在 Cache Manager service 尾部追加 create/get/list/cancel/pin/unpin/status methods。
- 在 Meta service 尾部追加内部 job/plan/pin mutation/query methods。
- 请求批量上限、分页 cursor、逐项结果和 protocol capability 固定。
- 更新 stubs、mock 和 service registration。

测试：method ID 唯一、旧/新 serde、batch limit、feature disabled。

建议提交：`feat(cache): define orchestration service contracts`

### T3：审计并分配 FDB KeyPrefix

- 审计所有现存 prefix 数值。
- 追加 PrefetchJob、PrefetchPlan、PinByBlock、PinByOwner prefix。
- 记录 key 格式、范围边界和最大 value 大小。
- 禁止复用或改变已有 prefix。

测试：key pack/unpack、prefix range 不相交、旧 key 解码不受影响。

建议提交：`feat(meta): reserve orchestration keyspace`

M0 退出：协议可编译，Phase 3 默认关闭，无持久化 mutation 可从外部触发。

## 4. M1：持久化地基

### T4：实现 PrefetchJobStore

- 新增 `src/meta/store/cache/PrefetchJobStore.*`。
- create 以 caller JobId 幂等；不同 spec 重用 ID 返回冲突。
- get/list 使用稳定 cursor；状态更新以 stateVersion CAS。
- 终态不可被普通更新重新打开。

测试：timeout 重试、spec 冲突、并发 CAS、分页、终态 fence。

建议提交：`feat(meta): persist prefetch jobs`

### T5：实现 PrefetchPlanStore

- 主键 `(jobId,inode,block)` 去重。
- append page 与 planned bytes/count 更新同事务。
- 保存 source index/cursor、block length、entry state 和 admission identity。
- planned bytes/count 溢出 fail closed。

测试：重复页、重叠 source、尾块、cursor 恢复、计数精确。

建议提交：`feat(meta): persist prefetch block plans`

### T6：实现双索引 PinStore

- 持久化 block-owner 与 owner-block 两个索引。
- create/renew/remove 幂等并校验两个索引一致。
- active 查询使用 Metadata server wall clock；过期记录视为无效。
- 限制单 block owner 数和单 owner block 数。

测试：重叠 owner、TTL、重复 unpin、索引不一致 fail closed。

建议提交：`feat(meta): persist owner scoped cache pins`

### T7：实现 Metadata job/plan/pin operations

- 接入 MetaStore、MetaOperator、Serde service 和 MetaClient。
- 用户 API 由 Cache Manager 鉴权，内部 mutation 要求 service identity。
- Job 状态、plan page、pin 和 cache block fence 在需要时同事务更新。

测试：权限、协议版本、事务冲突、逐项结果和 service token 脱敏。

建议提交：`feat(meta): expose orchestration operations`

M1 退出：Job/plan/pin 可持久化、分页查询和重启读取，但尚不执行加载。

## 5. M2：Planner

### T8：实现 Planner 抽象与 factory

- 新增 `planner/SourcePlanner.h`、context、page 和 factory。
- Planner 只产出冻结 inode/block，不直接调度 Loader。
- 未知 source type 或配置启动失败。

测试：factory、page bounds、cancel token、cursor round-trip。

建议提交：`feat(cache-manager): add source planner boundary`

### T9：实现 Namespace 文件 Planner

- stat OriginFile，拒绝普通文件、superseded 和不可缓存 layout。
- 按真实 file length 生成 blockLength。
- 冻结 inode/object version；重复路径由 PlanStore 去重。

测试：空文件、尾块、refresh 后旧 inode、权限和非法 layout。

建议提交：`feat(cache-manager): plan origin namespace files`

### T10：实现目录与 Path List Planner

- 目录递归走稳定分页 list；path list 有硬上限。
- 保存目录 cursor 和当前 child source cursor。
- symlink/循环行为沿用 Metadata path lookup，不自行遍历 inode 图。

测试：深目录、分页边界、重叠目录、取消和重启续跑。

建议提交：`feat(cache-manager): expand namespace datasets`

### T11：实现 Manifest v1 parser

- 一行一个绝对 namespace path，支持空行和 `#` 注释。
- 限制 bytes、行长、行数和展开 blocks。
- manifest Origin Range GET 使用冻结 VersionId/If-Match。

测试：分段行、UTF-8、CRLF、重复行、超限、版本变化。

建议提交：`feat(cache-manager): plan namespace manifests`

### T12：扩展 ObjectStore 分页 List API

- 在 ObjectStore 抽象增加 list request/page/object metadata。
- S3 使用 ListObjectsV2；token 必须推进，结果稳定排序。
- RoutedObjectStore 和 fake 实现同步更新。

测试：多页、空页终止、重复 token、错误映射、MinIO。

建议提交：`feat(cache): list origin objects by prefix`

### T13：实现 S3 Prefix import Planner

- 验证 destination root 和 key 映射不可逃逸。
- 对每个 object 调用幂等 ImportOriginFile，再交 Namespace Planner。
- prefix cursor 与 import/plan page 一起持久化。
- 不执行 delete propagation。

测试：分页、特殊 key、版本更新、重复页、部分 import timeout。

建议提交：`feat(cache-manager): plan object prefixes`

M2 退出：四类 source 均可生成可恢复且去重的完整 plan。

## 6. M3：Admission 与 Job Scheduler

### T14：扩展 AdmissionContext 支持显式 Prefetch

- context 增加 reason、priority、jobId、explicit/pin flags。
- PREFETCH/PIN 绕过 second-miss，普通 foreground 行为不变。
- 所有路径仍必须经过 Phase-2 rollout 和 physical preflight。

测试：显式首请求 admission、高水位拒绝、foreground 回归。

建议提交：`feat(cache-manager): admit explicit prefetch safely`

### T15：给 Hint 增加 Job claim 与完成回调

- LoadHint 保存有界 Job claim 集合。
- 相同 block 合并 owner，priority 取最大。
- 完成/失败通知所有 owner；取消单 owner 不破坏共享 hint。

测试：foreground+两个 Job 合并、owner 上限、取消其中一个。

建议提交：`feat(cache-manager): track shared load claims`

### T16：实现严格优先级 JobQueue

- 大 priority 优先、同 priority FIFO。
- contiguous batching 要求兼容 priority/owner quota。
- 防止低优先级相邻 block 搭便车。

测试：优先级、FIFO、batch boundary、priority upgrade。

建议提交：`feat(cache-manager): schedule prefetch priorities`

### T17：实现 per-Job 并发与带宽配额

- 每 Job inflight 不超过 spec。
- token bucket 使用可注入 monotonic clock。
- 全局/per-Origin CapacityGate 继续先于实际 S3 请求生效。
- 配额等待不占 Storage permit。

测试：并发、token refill、取消等待、不同 Job 隔离。

建议提交：`feat(cache-manager): enforce prefetch job quotas`

### T18：实现 JobRunner admission/attach

- 从 plan page 取下一批，调用显式 admission。
- READY 立即完成 entry；QUEUED/LOADING attach；失败分类重试或终止。
- admission permit/attempt identity 持久化，ambiguous timeout 可恢复。

测试：READY/QUEUED/LOADING、容量不足、attach 竞争和重启。

建议提交：`feat(cache-manager): execute durable prefetch plans`

### T19：接入 CacheManager lifecycle

- start 加载非终态 Job，stop 停止新调度并 drain 状态写入。
- BackgroundRunner 分离 planner、runner、tracker 周期。
- `enable_phase3=false` 时 API 返回 FeatureDisabled。

测试：启动回滚、重复 stop、恢复多个状态、Phase-2 未启用。

建议提交：`feat(cache-manager): run orchestration workers`

M3 退出：显式 Job 能按优先级和配额使用现有安全加载闭环。

## 7. M4：Ready Ratio 与 Cancel

### T20：实现 generation-fenced JobTracker

- 批量查询 plan block 的 Metadata cache state。
- 只有 matching READY 计入 ready bytes；更新 entry 与 counters 同事务。
- current ratio 可重新计算，achieved READY 单调。

测试：尾块、重复通知、generation 变化、溢出和分页。

建议提交：`feat(cache-manager): track prefetch readiness`

### T21：实现 Ready Ratio 状态机

- 使用基点整数交叉乘法，无浮点。
- 0 planned bytes -> FAILED/EMPTY_PLAN。
- LOADING/PARTIAL_READY/READY 转换 CAS，READY 只触发一次。

测试：0、1、9999、10000 bps，边界一字节，并发 tracker。

建议提交：`feat(meta): fence prefetch ready transitions`

### T22：实现 Cancel 收敛

- 先持久化 CANCELLED/cancel epoch。
- 阻止未 admission entries；删除该 Job claim。
- 仅对该 Job 独占且未 acquire 的 QUEUED attempt 做 fenced cancel。
- 重复 cancel 幂等，已开始 write 继续收敛。

测试：规划中、配额等待、共享 queued、loading、timeout 重试。

建议提交：`feat(cache-manager): cancel unstarted prefetch work`

M4 退出：用户可依赖 READY 门槛并安全取消未开始工作。

## 8. M5：Pin

### T23：活动 Job Pin 与续租

- plan entry 创建 ACTIVE_JOB owner pin。
- Manager 周期续租；重启从持久化 Job 恢复。
- CANCELLED/FAILED 删除 owner；READY 根据 spec 转换或删除。

测试：重启窗口、续租失败、重叠 Job、终态清理。

建议提交：`feat(cache-manager): protect active prefetch jobs`

### T24：Eviction 集成 Pin fence

- candidate 查询排除有效 pin。
- BeginEvict 事务再次检查 pin，关闭 scan/evict race。
- 本地安全驱逐同样遵守 pin；空间完全被 pin 占用时 fail closed 并报告。

测试：正常/本地驱逐、竞争、过期、最后 owner 删除。

建议提交：`feat(cache): fence eviction against active pins`

### T25：显式 Pin/Unpin 与 READY 后 TTL

- PinDataset 复用 Planner，可选 prefetchMissing。
- READY 后 ACTIVE_JOB pin 原子转换为 EXPLICIT/POST_READY TTL。
- Unpin 只删除 owner，status 返回 planned/pinned/ready bytes。

测试：pin-before-ready、TTL、重复 unpin、共享 owner、容量不足。

建议提交：`feat(cache-manager): manage dataset pin lifecycles`

M5 退出：Pin 语义在正常和本地安全驱逐下都可执行。

## 9. M6：API、CLI、指标与发布

### T26：实现 Cache Manager 公共 Job API

- create/get/list/cancel/pin/unpin/status handler。
- user ownership 和 admin visibility；service token 不出现在输出。
- create 使用 caller JobId 幂等。

测试：认证、ownership、分页、retry、feature/capability gate。

建议提交：`feat(cache-manager): expose orchestration api`

### T27：实现 Admin CLI

- `cache-prefetch create|status|list|cancel`。
- `cache-pin create|status|remove`。
- 支持 path/list/manifest/prefix 参数和 JSON/TOML spec 文件。
- 输出 job ID、状态、bytes、ratio、priority、错误和 pin expiry。

测试：命令注册、参数互斥、ratio 转换、secret redaction。

建议提交：`feat(admin): manage cache orchestration jobs`

### T28：增加指标与诊断状态

- planner、job state、queue/inflight、quota wait、ratio、cancel、pin 指标。
- identity tags 使用 job/inode/block 等安全字段，不记录对象凭证。
- cache-status 汇总非终态 Job、pin bytes 和失败 reason。

测试：指标名称、bounded reason、状态 serde 和旧 CLI 兼容。

建议提交：`feat(cache): observe data orchestration`

### T29：实现 Phase-3 capability、drain 与回滚门禁

- schema/protocol 只追加升级。
- enable 要求 Phase 2 ENABLED 且 Manager/Meta/CLI 能力完整。
- disable 前要求无非终态 Job、无 ACTIVE_JOB pin、无独占 queued claim。
- 写运行手册。

测试：半升级、retry、直接 disable、遗留 Job/pin fail closed。

建议提交：`feat(mgmtd): gate phase three orchestration rollout`

## 10. M7：验收

### T30：端到端与故障矩阵

- MinIO：path/list/manifest/prefix、优先级、ratio、cancel、pin TTL。
- 故障：planner page commit 前后、List token、import timeout、admission timeout、Manager restart、READY transition、cancel race。
- 验证 Phase-1/2 foreground fallback、capacity、eviction/event 套件无回归。
- 写 acceptance record，记录环境与未满足的部署测试。

建议提交：`test(cache): qualify phase three orchestration`

完成条件：T0..T29 有实现和窄测试；MinIO 与故障矩阵通过；Phase 3 默认关闭且可安全 drain/rollback。
