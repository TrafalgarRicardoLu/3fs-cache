# 3FS Day 4 Reading Guide

## 目标

今天的目标是读懂控制面 `mgmtd`，理解 3FS 如何维护集群成员、target 状态、chain table、routing info，以及故障时状态如何传播。

到今天结束时，你应该能讲清楚“一个 storage target 掉线后，系统如何把这件事传播到客户端”。

## 今日学习路径

建议按下面顺序执行：

1. `src/mgmtd/mgmtd.cpp`
2. `src/mgmtd/MgmtdServer.h`
3. `src/mgmtd/service/MgmtdOperator.h`
4. `src/mgmtd/service/MgmtdState.*`
5. `src/mgmtd/service/RoutingInfo.*`
6. `src/mgmtd/service/updateChain.*`
7. `src/mgmtd/background/`
8. `src/mgmtd/ops/`
9. 回看 `docs/design_notes.md` 中 failure detection 和 chain 状态机部分

今天一定要边读边画状态流转图。

## 分阶段任务

### 第一阶段：看入口和 Operator

建议用时：

- 45 到 60 分钟

任务：

- [ ] 读完 `src/mgmtd/mgmtd.cpp`
- [ ] 读完 `src/mgmtd/MgmtdServer.h`
- [ ] 读完 `src/mgmtd/service/MgmtdOperator.h`
- [ ] 总结 `mgmtd` 的对外职责

阶段产出：

- 一段对 `mgmtd` 职责边界的总结

### 第二阶段：看核心状态

建议用时：

- 50 到 70 分钟

任务：

- [ ] 理清 `MgmtdState`
- [ ] 理清 `RoutingInfo`
- [ ] 理清 `updateChain`
- [ ] 写出 chain version、target state、routing info version 的区别

阶段产出：

- 一张核心状态对象关系图

### 第三阶段：看后台任务和管理操作

建议用时：

- 40 到 50 分钟

任务：

- [ ] 扫完 `background/` 中主要后台任务
- [ ] 扫完 `ops/` 中主要管理操作
- [ ] 对照设计文档补齐故障状态机理解
- [ ] 画出 target 故障后的状态传播路径

阶段产出：

- 一张 target 从健康到故障再到恢复的状态流转图

## 今日练习题

1. `mgmtd` 维护的核心状态对象有哪些？
2. 心跳在 3FS 里除了“保活”还有什么作用？
3. `RoutingInfo` 为什么必须被客户端周期性刷新？
4. chain table 和 routing info 是同一件事吗？
5. target 的 public state 和 local state 有什么区别？
6. 为什么 target 出故障后会被移到链尾？
7. 一个 storage 节点故障后，谁负责检测、谁负责改链、谁负责广播？
8. 为什么 `mgmtd` 的后台任务拆成多个 checker/updater，而不是一个大循环？

答题要求：

- 每题至少写 2 句
- 第 2、3、5、7 题要重点认真写

## 进入 Day 5 前的门槛

满足下面 4 项，再进入 Day 5：

- [ ] 我已经读完 `mgmtd` 入口、核心状态和后台任务
- [ ] 我已经独立回答完 8 道练习题
- [ ] 我已经画出 target 故障后的状态传播图
- [ ] 我已经能解释 routing info 为什么影响客户端选路

## 笔记模板

```md
# Day 4

## 控制面职责
- membership:
- chain table:
- routing info:
- config:

## 核心状态对象
- MgmtdState:
- RoutingInfo:
- target public state:
- target local state:

## 主状态流转
- 心跳 -> 状态更新:
- 故障 -> 改链 -> 广播:

## 还不清楚的问题
- Q1:
- Q2:
- Q3:
```
