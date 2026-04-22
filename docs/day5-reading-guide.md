# 3FS Day 5 Reading Guide

## 目标

今天的目标是读懂 `meta`，理解文件系统语义如何建立在事务型 KV 上，重点是 inode、目录项、session 和事务边界。

到今天结束时，你应该能挑一个操作，例如 `rename` 或 `remove`，解释它是如何通过事务保护一致性的。

## 今日学习路径

建议按下面顺序执行：

1. `src/meta/meta.cpp`
2. `src/meta/service/MetaServer.h`
3. `src/meta/service/MetaOperator.h`
4. `src/meta/store/MetaStore.h`
5. `src/meta/store/Inode.*`
6. `src/meta/store/DirEntry.*`
7. `src/meta/store/PathResolve.*`
8. `src/meta/store/FileSession.*`
9. `src/meta/components/`
10. `src/fdb/HybridKvEngine.h`
11. `src/fdb/FDB*.{h,cc}`

今天一定要抓住“语义层”和“事务层”的边界。

## 分阶段任务

### 第一阶段：看 Server 和 Operator

建议用时：

- 45 到 60 分钟

任务：

- [ ] 读完 `meta` 入口和 `MetaServer`
- [ ] 读完 `MetaOperator`
- [ ] 总结 `MetaOperator` 在体系中的位置

阶段产出：

- 一段对 `MetaOperator` 职责的总结

### 第二阶段：看 MetaStore 和核心元数据结构

建议用时：

- 70 到 90 分钟

任务：

- [ ] 读完 `MetaStore`
- [ ] 读完 `Inode`、`DirEntry`、`PathResolve`
- [ ] 读完 `FileSession`
- [ ] 写出 inode、dir entry、file session 三者分别负责什么

阶段产出：

- 一份核心元数据结构职责表

### 第三阶段：看组件和 FDB 适配

建议用时：

- 40 到 50 分钟

任务：

- [ ] 扫一遍 `meta/components/`
- [ ] 补读 `HybridKvEngine` 和 FDB 适配层
- [ ] 为 `create/open/rename/remove` 各写一句事务层总结

阶段产出：

- 一段对 `MetaOperator` / `MetaStore` / `FDB` 三层关系的总结

## 今日练习题

1. `MetaOperator` 和 `MetaStore` 分别偏“编排层”还是“存储语义层”？
2. inode 和目录项为什么要拆成两套结构，而不是存在一个对象里？
3. 为什么目录项天然适合用范围扫描实现 `listdir`？
4. 为什么 `rename` 在分布式文件系统里是一个复杂操作？
5. 删除一个已打开文件时，为什么不能简单立刻删掉相关 chunk？
6. `SessionManager` 解决的核心问题是什么？
7. `PathResolve` 只是在做字符串处理吗？为什么不是？
8. `HybridKvEngine` 为什么存在，而不是直接 everywhere 只用 FDB？
9. 哪些地方体现了“meta 服务无状态，但语义仍然完整”？

答题要求：

- 每题至少写 2 句
- 第 2、4、5、9 题要重点认真写

## 进入 Day 6 前的门槛

满足下面 4 项，再进入 Day 6：

- [ ] 我已经读完 `meta` 入口、Operator、Store、核心元数据结构和 FDB 适配层
- [ ] 我已经独立回答完 9 道练习题
- [ ] 我已经能解释 inode、dir entry、file session 的边界
- [ ] 我已经能挑一个元数据操作解释它的事务保护方式

## 笔记模板

```md
# Day 5

## 语义层与事务层
- MetaOperator:
- MetaStore:
- FDB:

## 核心元数据结构
- inode:
- dir entry:
- file session:
- path resolve:

## 操作链路
- create:
- open:
- rename:
- remove:

## 还不清楚的问题
- Q1:
- Q2:
- Q3:
```
