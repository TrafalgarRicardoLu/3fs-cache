# 3FS Day 6 Reading Guide

## 目标

今天的目标是读懂 `storage`，把 CRAQ 写路径、读路径、版本状态和恢复过程串成一条完整链。

到今天结束时，你应该能完整讲出“一个 chunk 更新时，三副本链上每个节点本地状态如何变化”。

## 今日学习路径

建议按下面顺序执行：

1. `src/storage/storage.cpp`
2. `src/storage/service/StorageServer.h`
3. `src/storage/service/Components.h`
4. `src/storage/service/StorageOperator.*`
5. `src/storage/store/ChunkStore.*`
6. `src/storage/store/ChunkMetaStore.*`
7. `src/storage/store/ChunkReplica.*`
8. `src/storage/store/StorageTargets.*`
9. `src/storage/service/ReliableForwarding.*`
10. `src/storage/service/ReliableUpdate.*`
11. `src/storage/sync/ResyncWorker.*`
12. `src/storage/worker/`
13. 回看 `docs/design_notes.md` 中 CRAQ、recovery 部分
14. 补读 `src/storage/chunk_engine/README.md`

今天要边读边画写路径和恢复路径。

## 分阶段任务

### 第一阶段：看入口、装配和 Operator

建议用时：

- 50 到 70 分钟

任务：

- [ ] 读完 `storage` 入口和 `StorageServer`
- [ ] 读完 `Components`
- [ ] 读完 `StorageOperator`
- [ ] 总结 storage 服务的装配关系

阶段产出：

- 一张 `storage` 装配图

### 第二阶段：看 chunk 存储结构

建议用时：

- 60 到 80 分钟

任务：

- [ ] 读完 `ChunkStore` 及相关 store
- [ ] 理清 committed / pending 版本语义
- [ ] 总结 `ChunkStore`、`ChunkMetaStore`、`ChunkReplica` 的边界

阶段产出：

- 一张 chunk 版本与存储结构关系图

### 第三阶段：看写路径和恢复路径

建议用时：

- 60 到 80 分钟

任务：

- [ ] 读完 `ReliableForwarding`、`ReliableUpdate`
- [ ] 读完 `ResyncWorker`
- [ ] 扫一遍 `worker/` 目录
- [ ] 对照设计文档复核 CRAQ 流程
- [ ] 补读 `chunk_engine` 设计说明
- [ ] 画出一次写请求的状态图

阶段产出：

- 一张 head -> tail -> ack 的状态图

## 今日练习题

1. `StorageServer` 真正的“装配中心”为什么是 `Components`？
2. committed version 和 pending version 分别代表什么？
3. 为什么写操作必须在链头串行化？
4. 为什么 tail commit 之后还要 ack 回传？
5. 读请求遇到 pending version 时，为什么实现上没有直接去 tail 查版本？
6. `ReliableForwarding` 解决的是哪类失败场景？
7. target 恢复时，为什么需要 `dump-chunkmeta` 和数据同步两个阶段？
8. `ChunkStore`、`ChunkMetaStore`、`ChunkReplica` 的职责分别是什么？
9. Rust `chunk_engine` 的设计说明，跟 C++ storage 层的哪个问题域最相关？

答题要求：

- 每题至少写 2 句
- 第 2、3、4、7 题要重点认真写

## 进入 Day 7 前的门槛

满足下面 4 项，再进入 Day 7：

- [ ] 我已经读完 `storage` 入口、装配层、store 层、写路径和恢复路径
- [ ] 我已经独立回答完 9 道练习题
- [ ] 我已经画出 head -> tail -> ack 的状态图
- [ ] 我已经能解释 committed / pending 和恢复流程的关系

## 笔记模板

```md
# Day 6

## 装配关系
- StorageServer:
- Components:
- StorageOperator:

## 核心存储对象
- ChunkStore:
- ChunkMetaStore:
- ChunkReplica:
- StorageTargets:

## 主链路
- 写路径:
- 读路径:
- 恢复路径:

## 还不清楚的问题
- Q1:
- Q2:
- Q3:
```
