# 3FS 7-Day Code Reading Plan

## 目标

这份文档用于帮助第一次系统阅读 3FS 源码的人，在 7 天内建立一套稳定的整体认知：

- 系统里有哪些核心进程
- 控制面、元数据面、数据面分别由哪些模块负责
- 一次文件操作如何从客户端一路走到后端服务
- 关键一致性语义和故障恢复逻辑落在哪些代码里

这不是“逐行精读所有文件”的计划，而是“先建立可工作的心智模型，再逐层加细节”的计划。

## 阅读假设

- 目标读者熟悉 C++ 工程，但对 3FS 业务域不熟
- 每天可投入 2 到 3 小时
- 第一轮阅读优先建立系统图，不追求把所有工具类都读完

## 使用方式

每天只做三件事：

1. 按当天列出的顺序读文件
2. 回答当天的检查题
3. 写下当天产出，确认自己已经形成稳定结论

建议把每天的执行顺序固定为：

- 先读文档和头文件，后读实现
- 先抓职责和调用链，后看细节
- 先完成当天检查题，再决定是否补读

建议每天都维护一页自己的笔记，只保留四栏：

- 模块职责
- 关键对象
- 主调用链
- 仍然不清楚的问题

## 第一轮阅读时可以先跳过的内容

- `third_party/`
- `build/`
- `CMakeFiles/`
- `licenses/`
- 大多数 `src/common/utils/` 的实现细节
- 与当前主链路无关的测试辅助代码

---

## Day 1: 建立全局系统图

### 目标

先弄清楚 3FS 是一个怎样的分布式存储系统，不碰实现细节，只建立组件关系图和请求主路径。

### 建议用时

- 2 小时
- 如果 `docs/design_notes.md` 是第一次读，预留到 3 小时也正常

### 阅读顺序

1. `README.md`
2. `docs/design_notes.md`
3. `tests/fuse/run.sh`

### 执行清单

- [ ] 读完 `README.md`
- [ ] 读完 `docs/design_notes.md` 的系统设计总览
- [ ] 通读 `tests/fuse/run.sh`，确认最小集群启动顺序
- [ ] 画出四大组件关系图
- [ ] 写出一个文件读请求的高层路径
- [ ] 回答完今天的检查题

### 重点关注

- 系统中有哪些长期运行的进程
- 哪些模块属于控制面，哪些属于数据面
- 元数据和数据分别落在哪种存储中
- 启动一个最小测试集群需要哪些组件

### 今天产出

- 画出四大组件关系图：`mgmtd`、`meta`、`storage`、`client/fuse`
- 写出一个最小集群的启动顺序
- 写出一个“文件读请求”的高层路径

### 检查题

1. 3FS 的四个核心组件分别是什么，各自职责是什么？
2. 为什么 `meta` 被设计为无状态服务？
3. 为什么文件元数据放在事务型 KV，而文件数据不放在那里？
4. `mgmtd` 为什么在整个系统里是控制中枢？
5. `tests/fuse/run.sh` 里为什么要先启动 FoundationDB，再启动 `mgmtd`？
6. `tests/fuse/run.sh` 里创建 targets 和 upload chain table 的动作，本质上是在初始化什么？
7. 一个应用通过 FUSE 访问 3FS 时，最先接触到的用户态进程是谁？
8. 文档里说的 native client 和 FUSE client，有什么角色差异？

### 完成标准

你可以不看代码，口头讲清楚：

- 这个系统有哪些进程
- 它们之间怎么依赖
- 请求为什么不会直接从客户端打到 FoundationDB

### 收工前勾选

- [ ] 我能说清 `mgmtd`、`meta`、`storage`、`fuse/client` 的职责
- [ ] 我知道最小测试集群为什么要先起 FDB
- [ ] 我能解释 chain table 初始化在系统里意味着什么

### Day 1 细化执行版

Day 1 的细化学习脚本已经拆到独立文件：

- [docs/day1-reading-guide.md](/data00/home/lujianhui.1/3FS/docs/day1-reading-guide.md:1)

建议用法：

- 先在本文件完成 Day 1 总目标确认
- 再打开独立 Day 1 指南按步骤执行
- 完成后回到这里，把 Day 1 的勾选项打掉

---

## Day 2: 吃透公共启动骨架

### 目标

理解一个 3FS 服务是怎样被创建、配置、启动并挂载 RPC 的。

Day 2 的细化学习脚本已经拆到独立文件：

- [docs/day2-reading-guide.md](/data00/home/lujianhui.1/3FS/docs/day2-reading-guide.md:1)

---

## Day 3: 用 simple_example 学会读服务模板

### 目标

在最小样板里看清一个服务的完整形状，然后把这套模式迁移到真正的业务服务上。

Day 3 的细化学习脚本已经拆到独立文件：

- [docs/day3-reading-guide.md](/data00/home/lujianhui.1/3FS/docs/day3-reading-guide.md:1)

---

## Day 4: 控制面 mgmtd

### 目标

搞清楚 3FS 如何维护集群成员、心跳、路由信息、chain table，以及节点故障时状态如何传播。

Day 4 的细化学习脚本已经拆到独立文件：

- [docs/day4-reading-guide.md](/data00/home/lujianhui.1/3FS/docs/day4-reading-guide.md:1)

---

## Day 5: 元数据面 meta

### 目标

搞清楚文件系统语义是如何建立在事务型 KV 之上的，重点理解 inode、目录项、session 和事务边界。

Day 5 的细化学习脚本已经拆到独立文件：

- [docs/day5-reading-guide.md](/data00/home/lujianhui.1/3FS/docs/day5-reading-guide.md:1)

---

## Day 6: 数据面 storage

### 目标

把 CRAQ 写路径、读路径、版本状态和恢复过程串成一条完整链。

Day 6 的细化学习脚本已经拆到独立文件：

- [docs/day6-reading-guide.md](/data00/home/lujianhui.1/3FS/docs/day6-reading-guide.md:1)

---

## Day 7: 客户端、FUSE 与 zero-copy 路径

### 目标

把用户请求入口到后端服务的主链路闭环起来，并区分普通 FUSE 路径和高性能 native zero-copy 路径。

Day 7 的细化学习脚本已经拆到独立文件：

- [docs/day7-reading-guide.md](/data00/home/lujianhui.1/3FS/docs/day7-reading-guide.md:1)

---

## 每天结束时统一自测

每天结束前，统一回答下面 5 个问题。如果其中 2 个答不上来，第二天不要往后赶进度。

1. 我今天读的模块，在整个系统里的职责是什么？
2. 这个模块最核心的 2 到 3 个对象是什么？
3. 一个请求经过这个模块时，输入是什么，输出是什么？
4. 这个模块最重要的一致性或性能约束是什么？
5. 如果这个模块坏了，系统会表现出什么问题？

### 每日统一勾选模板

- [ ] 我今天已经按顺序读完计划中的核心文件
- [ ] 我已经回答完当天检查题
- [ ] 我已经写下当天的模块职责和主调用链
- [ ] 我已经标出至少 3 个还不清楚的问题
- [ ] 我知道明天阅读要承接今天的哪个结论

---

## 七天结束后的复盘题

如果 7 天读完后，下面这些题你能答到 70% 以上，就说明第一轮通读是有效的。

1. 为什么 3FS 要把控制面、元数据面、数据面拆成不同服务？
2. 为什么 `meta` 可以无状态而 `storage` 不能无状态？
3. `mgmtd` 下发的 routing info 对 `meta` 客户端和 `storage` 客户端分别有什么意义？
4. 一个文件从创建到第一次写入，元数据和数据分别新增了哪些信息？
5. 一个 chunk 写入失败，在哪些层会看到重试或状态变化？
6. 为什么 CRAQ 特别适合读多写少或读吞吐要求高的场景？
7. 删除、rename、session、file length 更新，哪一个在元数据层最容易出错，为什么？
8. 如果要排查“读延迟抖动”，你会先看 `mgmtd`、`meta`、`storage`、`fuse` 里的哪几块？
9. 如果要新增一个后台服务，最值得参考的代码模板是哪套？
10. 如果要新增一个客户端侧功能，应该优先考虑挂在 `MetaClient` 还是 `StorageClient`，判断依据是什么？

---

## 第二轮阅读建议

第一轮结束后，如果你要继续深入，建议按兴趣拆成三条路线：

- 一致性路线：继续深挖 `meta/store`、`storage/service`、`storage/sync`
- 性能路线：继续深挖 `fuse`、`StorageClient`、`USRBIO`、`chunk_engine`
- 运维路线：继续深挖 `mgmtd/background`、`client/cli/admin`、`deploy/`、`configs/`

如果只打算做 feature 开发，第二轮最先补：

- `src/client/cli/admin/`
- `src/meta/components/`
- `src/storage/service/`
- `src/fuse/FuseOps.cc`

## 建议节奏

如果你希望这份计划真正可执行，建议用下面的节奏：

- 工作日版本：每天 2 小时，连续 7 天
- 周末版本：工作日只看文档和头文件，周末补实现
- 压缩版本：第 3 天和第 7 天可以合并，但不要压缩第 4、5、6 天

最不建议的做法：

- 第一天就从 `src/common/utils/` 开始
- 不看设计文档直接钻 `meta/store`
- 一边读一边大面积跳文件，没有形成当天结论

## 附：建议配套阅读命令

在本仓库里阅读时，优先用这些命令：

```bash
rg "TwoPhaseApplication|ServerLauncher|MetaOperator|StorageOperator|MgmtdOperator" src
rg --files src/meta src/storage src/mgmtd src/fuse
sed -n '1,220p' <file>
```

避免第一轮就做这些事：

```bash
rg "" src/common/utils
rg "" third_party
```

因为信息量太大，而且会稀释主链路理解。
