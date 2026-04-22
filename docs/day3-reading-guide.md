# 3FS Day 3 Reading Guide

## 目标

今天的目标是用 `simple_example` 吃透 3FS 服务模板，理解一个最小服务由哪些部分组成，以及这些部分如何映射到正式服务。

到今天结束时，你应该能把 `simple_example` 的模式直接套到 `mgmtd`、`meta`、`storage` 上去看。

## 今日学习路径

建议按下面顺序执行：

1. 读 `src/simple_example/README.md`
2. 读 `src/simple_example/main.cpp`
3. 读 `src/simple_example/service/Server.h`
4. 读 `src/simple_example/service/Service.h`
5. 对照 `src/fbs/simple_example/`

今天的重点不是业务，而是模板。

## 分阶段任务

### 第一阶段：先看入口

建议用时：

- 20 到 30 分钟

任务：

- [ ] 读完 `src/simple_example/README.md`
- [ ] 读完 `src/simple_example/main.cpp`
- [ ] 总结最小服务的入口长什么样

阶段产出：

- 一段对最小服务入口结构的描述

### 第二阶段：看 Server 定义

建议用时：

- 35 到 45 分钟

任务：

- [ ] 读完 `src/simple_example/service/Server.h`
- [ ] 标出 `kName`、`kNodeType`、Config、Launcher 等关键点
- [ ] 总结“Server 类负责什么”

阶段产出：

- 一段对 `Server` 类职责的总结

### 第三阶段：看 Service 和 schema

建议用时：

- 30 到 40 分钟

任务：

- [ ] 读完 `src/simple_example/service/Service.h`
- [ ] 对照看 `src/fbs/simple_example/`
- [ ] 写出 schema 与实现的对应关系

阶段产出：

- 一张“schema -> service -> server -> main” 的映射关系

## 今日练习题

1. `simple_example` 的存在价值是什么？
2. `SimpleExampleServer` 和 `SimpleExampleService` 分别代表什么层级？
3. 业务 service 为什么不直接等于进程入口类？
4. `src/fbs/simple_example` 中的定义和 `service/Service.h` 是怎样对应的？
5. 为什么新增一个服务不只是复制 `src/simple_example`，还要补 `src/fbs/simple_example` 对应部分？
6. `kNodeType` 在系统中可能被哪些逻辑使用？
7. 如果你要快速看懂一个新服务，先读哪 3 个文件最划算？

答题要求：

- 每题至少写 2 句
- 第 2、3、4 题要重点认真写

## 进入 Day 4 前的门槛

满足下面 4 项，再进入 Day 4：

- [ ] 我已经读完 `simple_example` 的入口、Server、Service、schema
- [ ] 我已经独立回答完 7 道练习题
- [ ] 我已经能区分入口类、Server 类、业务 Service 类
- [ ] 我已经能说清新增服务至少要补哪些位置

## 笔记模板

```md
# Day 3

## 模板角色
- main:
- Server:
- Service:
- schema:

## 关键概念
- kName:
- kNodeType:
- Config:
- Launcher:

## 映射关系
- schema -> Service:
- Service -> Server:
- Server -> main:

## 还不清楚的问题
- Q1:
- Q2:
- Q3:
```
