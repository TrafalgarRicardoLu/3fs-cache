# 3FS Day 5 Reading Guide Answers

> 做完 [docs/day5-reading-guide.md](/data00/home/lujianhui.1/3FS/docs/day5-reading-guide.md:1) 里的阅读任务后再看这份参考产出。

## 阶段产出

### 第一阶段：MetaOperator 职责总结

`MetaOperator` 位于 RPC service 和 `MetaStore` 之间，是 meta 服务的编排层。`MetaSerdeService` 只负责把 `MetaSerde` RPC 转发到 `MetaOperator`，而 `MetaOperator` 负责认证、请求校验、事务驱动、跨 meta 转发、批处理、后台组件生命周期，以及把 `MetaStore` operation 放进 KV transaction 里执行。

它本身不直接定义 inode、dir entry 的 KV key，也不把每个元数据操作的细节全部写在 RPC 层。具体语义由 `MetaStore` 和 `meta/store/ops/*` 实现；`MetaOperator` 则决定这个请求应该本地执行、按 inode/parent 批处理，还是通过 `Forward` 转发到负责该 inode 的 meta 节点。

### 第二阶段：核心元数据结构职责表

| 结构 | 存储内容 | 主要职责 | 事务关注点 |
|---|---|---|---|
| `Inode` | 文件、目录、symlink 的对象属性；包括 id、acl、nlink、时间戳、文件 layout、目录 parent/name 等 | 表示文件系统对象本体，承载权限、类型、文件布局和目录层级信息 | 读改写 inode 时要加入 read conflict，防止并发删除、改属性或移动目录造成语义冲突 |
| `DirEntry` | `(parent inode id, name) -> child inode id/type/acl` 的命名索引 | 表示目录中的一个名字，连接路径名和 inode；也是 `listdir` 范围扫描的基础 | create/rename/remove 会对相关 dir entry 加 read conflict，防止并发创建同名项或覆盖错对象 |
| `PathResolveOp` | 不独立持久化；在事务中沿 dir entry/inode 解析路径 | 解析 `PathAt`、处理 root、`.`、symlink、父目录权限、缺失路径等语义 | 默认用 snapshot load，不自动加入 conflict；调用方要按操作语义手动添加需要保护的 inode/dir entry |
| `FileSession` | `(inode id, session id) -> client/session/timestamp` | 记录写打开的文件会话，避免已打开文件被删除后 chunk 生命周期失控 | 删除、truncate、GC、SessionManager prune 都要检查或清理 session，避免和活跃写入并发破坏一致性 |

`inode` 和 `dir entry` 分离后，一个 inode 可以通过多个目录项引用，目录 listing 也可以只扫某个 parent 前缀下的 dir entry。`file session` 则不属于命名空间结构，它记录的是“文件被哪个 client 以写方式打开”的运行期语义。

### 第三阶段：MetaOperator / MetaStore / FDB 三层关系

`MetaOperator` 是 RPC 编排和运行时组件层，负责认证、forward、batch、background components、storage/mgmtd client 协作。`MetaStore` 是元数据语义层，它把 `stat/open/create/rename/remove/list` 等操作建模成 `Operation`，并在 operation 内部组合 `Inode`、`DirEntry`、`FileSession`、`PathResolveOp`、`GcManager` 等结构。

FDB 适配层提供事务能力。`HybridKvEngine` 根据配置选择 mem KV 或 FDB，并返回统一的 `IReadWriteTransaction`；`OperationDriver` 创建 transaction，运行 `MetaStore` operation，处理 FDB retry、maybe committed、idempotent 记录和 commit。这样 meta 进程本身可以无状态，真正的元数据一致性落在事务型 KV 和明确的 conflict set 上。

create/open/rename/remove 的事务层总结：

- `create`：先解析父目录并检查权限，分配 inode id 和 layout，在同一事务里写入 dir entry、inode，必要时写入 file session，并对父 inode 和目标 dir entry 加 read conflict。
- `open`：读打开可以是 readonly transaction；写打开或 `O_TRUNC` 会进入写事务，检查权限、文件洞、session，并可能写 file session、更新 inode 或替换大文件 inode。
- `rename`：在一个事务里同时解析源和目标，检查权限、sticky bit、目录环、目标是否可替换，再删除源 entry、处理旧目标、创建新目标 entry，并对源/目标父目录和 entry 加 conflict。
- `remove`：在事务里解析目标、检查父目录写权限、sticky bit、immutable、目录是否为空；空目录可直接删 entry/inode，文件或递归删除交给 `GcManager` 移入 GC 路径，延后清理 chunk。

## 练习题参考答案

1. `MetaOperator` 和 `MetaStore` 分别偏“编排层”还是“存储语义层”？
   - `MetaOperator` 更偏编排层。它接住 RPC 后做认证、请求校验、选择本地执行还是 forward、创建事务、驱动 retry、管理后台组件和 batch。
   - `MetaStore` 更偏存储语义层。它把文件系统操作拆成 `Operation`，在 operation 中定义如何读写 inode、dir entry、file session，以及哪些 key/range 要加入事务冲突集。

2. inode 和目录项为什么要拆成两套结构，而不是存在一个对象里？
   - inode 表示对象本体，目录项表示命名关系。一个文件对象的属性、layout、nlink、权限属于 inode，而某个父目录下的名字属于 dir entry；两者生命周期和访问模式不同。
   - 拆开以后，hard link、多目录项指向同一 inode、rename 只移动名字、listdir 只扫描目录项这些语义都更自然。如果把它们塞进一个对象，目录范围扫描、并发 rename/remove 和多链接语义都会变得更别扭。

3. 为什么目录项天然适合用范围扫描实现 `listdir`？
   - `DirEntry` 的 key 格式是 prefix + parent inode id + name，同一个目录下的所有 entry 在 KV key 空间里天然连续。列目录时只需要扫描这个 parent 前缀对应的 key range。
   - 这和目录语义很匹配：`listdir(parent)` 本质就是枚举 parent 下按 name 排列的一段键值。需要 inode 详情时，再按 entry 里的 inode id 并发加载 inode。

4. 为什么 `rename` 在分布式文件系统里是一个复杂操作？
   - `rename` 不只是改一个名字，它同时涉及源父目录、目标父目录、源 entry、目标 entry、源 inode，有时还要删除或替换目标对象。若源是目录，还必须防止把目录移动到自己的子孙目录里形成环。
   - 它还要满足 POSIX 语义：目标可以不存在，也可能是可替换文件或空目录；sticky bit、immutable、目录锁、move-to-trash、idempotent retry 都会影响结果。因此它必须放进一个事务里统一检查和提交。

5. 删除一个已打开文件时，为什么不能简单立刻删掉相关 chunk？
   - 已打开文件仍可能有客户端继续写入、close、sync 或更新长度。如果 meta 立刻删除 chunk，活跃写会话可能继续产生新数据或提交长度，导致文件语义和底层数据生命周期不一致。
   - 3FS 用 `FileSession` 记录写打开会话，并用 `GcManager` 延后清理被移除文件的数据。这样命名空间可以先删除 entry，而 chunk 清理要等 session 和 GC 逻辑确认安全后再做。

6. `SessionManager` 解决的核心问题是什么？
   - `SessionManager` 解决的是客户端异常退出后写会话残留的问题。它通过 mgmtd 获取活跃 client session，再扫描 `FileSession`，找出死客户端留下的 session。
   - 找到死 session 后，它可以直接 prune session，或者走 close/sync 路径尽量把文件长度和状态收敛。没有这个机制，已删除但仍有残留 session 的文件可能长期阻塞 GC。

7. `PathResolve` 只是在做字符串处理吗？为什么不是？
   - 不是。`PathResolveOp` 虽然输入是路径，但它会在 KV 事务中逐级加载 dir entry/inode，处理 root、`.`、symlink、父目录权限、缺失中间路径、deleted directory 和 symlink 深度限制。
   - 它还故意使用 snapshot load，不自动加入 read conflict set。也就是说它提供“看到当前路径状态”的能力，真正要保护哪些并发变化，由 create/rename/remove 等调用方根据语义手动决定。

8. `HybridKvEngine` 为什么存在，而不是直接 everywhere 只用 FDB？
   - `HybridKvEngine` 给 meta 上层提供统一的 `IKVEngine` 接口，上层只需要创建 read/write transaction，不需要关心底层是 mem KV 还是 FDB。这样测试、兼容旧配置和真实部署可以共用同一套 meta 语义代码。
   - 它还把 FDB context、多 DB 实例选择和 mem fallback 包在一个地方。直接 everywhere 使用 FDB 会让业务层耦合 FoundationDB API，测试和替换底层实现也更困难。

9. 哪些地方体现了“meta 服务无状态，但语义仍然完整”？
   - meta 进程本身不把 inode 树、目录项或 file session 当成本地权威状态保存；这些都通过 `MetaStore` 写入事务型 KV。meta 重启后，只要能连接 KV 和 mgmtd，就能继续按 KV 中的数据恢复语义。
   - 语义完整来自事务边界和冲突集：create 同时写 inode/entry/session，rename 同时处理源目标和权限，remove 通过 GC/session 保证 chunk 生命周期。`MetaOperator` 里的缓存、allocator、session scan 和 routing info 都是运行期辅助，不是唯一真相。

## Day 5 笔记模板补全

```md
# Day 5

## 语义层与事务层
- MetaOperator: RPC 编排层，负责认证、forward、batch、组件生命周期和事务驱动。
- MetaStore: 元数据语义层，把每个文件系统操作实现成 Operation。
- FDB: 事务型 KV，提供原子提交、冲突检测、retry 和持久化状态。

## 核心元数据结构
- inode: 文件/目录/symlink 对象本体，保存属性、权限、layout、nlink 和时间戳。
- dir entry: 父目录下的名字索引，连接 parent/name 到 inode。
- file session: 写打开会话记录，用于 close/sync/prune/GC。
- path resolve: 带权限、symlink 和缺失路径语义的事务内路径解析器。

## 操作链路
- create: MetaOperator 认证和选择 parent owner -> BatchedOp -> 事务写 dir entry/inode/session。
- open: readonly open 可只读；写 open 写入 session，O_TRUNC 可能替换 inode 或要求 truncate。
- rename: 同事务解析源/目标 -> 检查权限和环 -> 删除旧 entry -> 处理目标 -> 创建新 entry。
- remove: 同事务解析目标和权限 -> 空目录直接删 -> 文件/递归删除交给 GC 延后清 chunk。

## 还不清楚的问题
- Q1: GcManager 如何把目录递归删除拆成多个可恢复任务？
- Q2: Distributor 的 inode 到 meta 节点映射在扩缩容时如何迁移？
- Q3: close/sync 如何和 storage 查询长度、文件洞、truncateVer 对齐？
```
