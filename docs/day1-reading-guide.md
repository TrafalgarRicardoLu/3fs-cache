# 3FS Day 1 Reading Guide

## 目标

这份文档是 3FS 7 天通读计划的 Day 1 细化执行版，目标不是读很多代码，而是先建立 3FS 的整体系统图。

到今天结束时，你应该能不看代码，直接讲清楚：

- 3FS 里有哪些长期运行的进程
- `mgmtd`、`meta`、`storage`、`fuse/client` 怎么协作
- 一个文件读请求为什么不会直接打到 FoundationDB

## 今日学习路径

如果你今天是第一次正式进入 3FS，建议严格按下面顺序执行：

1. 打开 `README.md`，读到 Documentation 一节为止
2. 写一句话回答：“3FS 本质上是一个什么系统？”
3. 打开 `docs/design_notes.md`，先连续读 15 到 20 分钟
4. 读到四个组件介绍后先停下来，写出组件职责
5. 再继续读 FUSE 限制和 zero-copy API
6. 最后读 `tests/fuse/run.sh`，把启动顺序整理成列表

这样安排的目的很明确：

- 先建立系统图
- 再理解设计约束
- 最后用启动脚本把抽象设计落到运行时顺序上

## 分阶段任务

### 第一阶段：读 `README.md`

建议用时：

- 15 分钟

阅读文件：

- `README.md`

任务：

- [ ] 读到 Documentation 一节为止
- [ ] 用自己的话写一句“3FS 本质上是什么系统”

只抓这 4 个问题：

- 3FS 属于什么类型的系统
- 它主要解决什么问题
- 四个核心组件分别是什么
- 为什么文档里会同时强调 FUSE 和 native client

阶段产出：

- 一句不超过 80 字的总结

### 第二阶段：读 `docs/design_notes.md`

建议用时：

- 60 到 80 分钟

阅读文件：

- `docs/design_notes.md`

今天重点看这些部分：

- `Design and implementation`
- `File system interfaces`
- `Asynchronous zero-copy API`
- `File metadata store`
- `Chunk storage system`
- `Failure detection`
- `Data recovery`

任务：

- [ ] 先读系统设计总览
- [ ] 读 FUSE 限制和 zero-copy API
- [ ] 读元数据建模部分
- [ ] 读 chunk replication 和 recovery 的高层设计
- [ ] 给 4 个核心组件各写一句职责

如果你读到 CRAQ 细节已经开始吃力，先记住概念，不要在今天死抠实现。

今天必须抓住这 6 个问题：

- `mgmtd` 管什么
- `meta` 管什么
- `storage` 管什么
- FUSE client 和 native client 的差别是什么
- 元数据为什么放在事务型 KV
- chunk 为什么走 replication chain

阶段产出：

- 四个组件的职责说明
- 一张简单的系统关系图草图

### 第三阶段：读 `tests/fuse/run.sh`

建议用时：

- 30 到 40 分钟

阅读文件：

- `tests/fuse/run.sh`

任务：

- [ ] 按顺序找出最小集群的启动阶段
- [ ] 写出启动顺序
- [ ] 标出 cluster 初始化相关的管理动作

你今天应该至少识别出这些步骤：

- 启动 FDB
- 初始化 cluster
- 启动 `mgmtd_main`
- 启动 `storage_main`
- 启动 `meta_main`
- 创建 targets 和 chain table
- 启动 `hf3fs_fuse_main`

阶段产出：

- 一份 6 到 8 行的启动顺序列表

## 今日练习题

1. 3FS 的四个核心组件分别是什么，各自职责是什么？
   - `mgmtd`：负责整个集群的控制面，维护节点 membership、target 状态、chain table、routing info，以及配置分发。
   - `meta`：负责文件系统元数据，主要包括目录树、inode、目录项、文件布局信息、session 等语义。
   - `storage`：负责文件数据存储，主要保存 chunk 数据，并通过 replication chain 提供一致性和恢复能力。
   - `fuse/client`：负责面向应用提供访问入口。FUSE client 提供文件系统接口，native client 提供高性能 zero-copy IO 路径。

2. 为什么 `meta` 被设计成无状态服务？
   - 因为 `meta` 的核心状态被下沉到事务型 KV，也就是 FoundationDB，所以 `meta` 实例本身不需要长期持有业务状态。
   - 这样做的好处是可以方便地横向扩展，也方便故障切换、重启和升级，客户端也可以连接任意可用的 `meta` 节点。

3. 为什么文件元数据放在事务型 KV，而 chunk 数据不放在那里？
   - 更本质的原因不是“一个小一个大”，而是它们的语义和访问模式不同。元数据操作需要强事务语义，比如 `create`、`rename`、`unlink`、目录项更新、session 管理等，这类操作适合放在事务型 KV 上。
   - chunk 数据的数据量和吞吐量都很大，更适合走独立的数据面，由 `storage` 服务通过 replication chain 来管理，而不是塞进事务型 KV。

4. `mgmtd` 为什么是控制中枢，而不是 `meta`？
   - 因为 `mgmtd` 掌握的是整个集群的全局运行状态，包括节点信息、target 状态、chain table、routing info 和配置版本。
   - `meta` 只负责文件系统语义层的元数据，不负责全局成员管理和数据面路由，所以它不能承担控制中枢的角色。

5. FUSE client 和 native client 的主要差别是什么？
   - FUSE client 提供标准 Linux 文件系统接口，应用接入成本低，可以像访问普通文件系统一样访问 3FS，但性能会受到 FUSE 路径本身的限制。
   - native client 提供异步 zero-copy IO 能力，适合性能敏感场景，尤其是小随机读等场景，性能比传统 FUSE 路径更强。

6. `tests/fuse/run.sh` 为什么先起 FDB，再做 `init-cluster`？
   - 因为 `init-cluster` 需要往底层 KV 写初始化数据，而这个底层 KV 就是 FDB，所以 FDB 必须先可用。
   - 这里要注意，`init-cluster` 主要做的是初始化文件系统根布局，以及写入 `mgmtd`、`meta`、`storage`、`fuse` 的配置。root 用户和 token 不是 `init-cluster` 本身写进去的，而是脚本前面的 `user-add` 和 `user-set-token` 先写进去的。

7. `create-target`、`upload-chains`、`upload-chain-table` 这三步本质上在初始化什么？
   - `create-target`：把实际可用的物理存储目标注册出来，可以理解成把“盘上的可分配坑位”建出来。
   - `upload-chains`：把这些 target 组织成复制链，也就是副本组。
   - `upload-chain-table`：把这些链挂到一个可被文件布局引用的分配表上，供后续文件创建和 chunk 分布使用。

8. 一个应用通过 FUSE 访问 3FS 时，最先接触到的用户态进程是谁？
   - 最先接触到的是 FUSE client daemon，也就是 3FS 的 FUSE 用户态进程。
   - 内核会把文件系统请求转发给它，再由它去继续走元数据路径或数据路径。

9. 一个文件读请求为什么不需要每次都经过 `mgmtd`？
   - 因为 `mgmtd` 主要负责提供和维护 routing info、集群拓扑、chain table 等全局控制信息，而不是处理每次读请求本身。
   - 客户端会缓存 routing info。文件在 `open` 时，client 从 `meta` 拿到文件布局信息，之后就可以根据布局和缓存的 routing info 计算 chunk 所在链路，并直接访问对应的 `storage` 节点。

10. 如果 `meta` 全挂了，和如果 `storage` 全挂了，系统表现会有什么本质区别？
   - 如果 `meta` 全挂了，文件系统语义入口会大面积失效，例如 `lookup/open/create/rename/readdir/chmod` 这些操作都会受影响。不能简单说“还能正常读已有文件”，因为很多读之前也需要先经过 metadata 路径。
   - 如果 `storage` 全挂了，数据面会瘫痪，文件内容无法正常读写；但元数据层本身可能还活着，一些纯命名空间或权限相关操作理论上仍可能工作。两者的本质区别是：一个是语义层不可用，一个是数据面不可用。

答题要求：

- 每题至少写 2 句
- 第 2、3、4、9 题要重点认真写

## 进入 Day 2 前的门槛

满足下面 4 项，再进入 Day 2：

- [ ] 我已经读完今天的 3 份材料
- [ ] 我已经独立回答完 10 道练习题
- [ ] 我已经写出系统定义、组件职责、启动顺序和读路径图
- [ ] 我已经整理出至少 3 个还没理解透的问题

## 笔记模板

建议每天都按这个固定格式记笔记：

```md
# Day 1

## 模块职责
- mgmtd:
- meta:
- storage:
- fuse/client:

## 关键对象/概念
- routing info:
- chain table:
- inode:
- chunk:
- native client:

## 主调用链
- 启动链路:
- 文件读请求链路:

## 还不清楚的问题
- Q1:
- Q2:
- Q3:
```
