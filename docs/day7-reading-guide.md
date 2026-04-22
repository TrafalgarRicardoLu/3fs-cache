# 3FS Day 7 Reading Guide

## 目标

今天的目标是把客户端、FUSE 和 zero-copy 路径串起来，完成“应用请求如何一路走到后端服务”的闭环理解。

到今天结束时，你应该能把一个应用发起的 `open + read + write + fsync` 请求，按模块一路讲到后端。

## 今日学习路径

建议按下面顺序执行：

1. `src/client/mgmtd/MgmtdClient.h`
2. `src/client/meta/MetaClient.h`
3. `src/client/storage/StorageClient.h`
4. `src/fuse/hf3fs_fuse.cpp`
5. `src/fuse/FuseApplication.*`
6. `src/fuse/FuseClients.*`
7. `src/fuse/FuseOps.cc`
8. `src/lib/api/hf3fs_usrbio.h`
9. `src/lib/api/UsrbIo.cc`
10. `src/lib/api/UsrbIo.md`

今天的重点是“主链路闭环”，不是再深入某个单点实现。

## 分阶段任务

### 第一阶段：看客户端三层

建议用时：

- 45 到 60 分钟

任务：

- [ ] 读完 `MgmtdClient`
- [ ] 读完 `MetaClient`
- [ ] 读完 `StorageClient`
- [ ] 总结三者的职责边界

阶段产出：

- 一张客户端职责边界图

### 第二阶段：看 FUSE 路径

建议用时：

- 50 到 70 分钟

任务：

- [ ] 读完 FUSE 入口和 `FuseApplication`
- [ ] 读完 `FuseClients`
- [ ] 挑重点读 `FuseOps.cc`
- [ ] 画出 `open/read/write/fsync` 的 FUSE 路径

阶段产出：

- 一张 FUSE 路径链路图

### 第三阶段：看 zero-copy API

建议用时：

- 40 到 50 分钟

任务：

- [ ] 读完 `hf3fs_usrbio.h`
- [ ] 结合 `UsrbIo.cc` 和 `UsrbIo.md` 理解 zero-copy API
- [ ] 总结 FUSE 路径和 USRBIO 路径的差别
- [ ] 写出为什么 native client 仍依赖 FUSE 进程

阶段产出：

- 一段对 FUSE 与 USRBIO 两条路径的对比总结

## 今日练习题

1. `MgmtdClient` 在客户端体系里主要提供什么能力？
2. `MetaClient` 和 `StorageClient` 分别对应哪一段业务语义？
3. 为什么客户端不能把所有操作都直接打到 storage？
4. FUSE 路径里最明显的性能瓶颈是什么？
5. `hf3fs_usrbio.h` 里的 `Iov` 和 `Ior` 分别在扮演什么角色？
6. native zero-copy API 为什么仍然依赖 FUSE 进程？
7. 为什么 `open/stat/close` 这类操作仍然保留在文件系统语义层，而不是完全下沉成裸数据 IO？
8. 什么时候一个应用更适合用 FUSE，什么时候更适合接 native API？

答题要求：

- 每题至少写 2 句
- 第 2、4、5、6 题要重点认真写

## 完成 Day 7 的门槛

满足下面 4 项，就说明第一轮主链路阅读已经闭环：

- [ ] 我已经读完客户端三层、FUSE 路径和 USRBIO 路径
- [ ] 我已经独立回答完 8 道练习题
- [ ] 我已经画出 `open/read/write/fsync` 闭环链路
- [ ] 我已经能解释 FUSE 路径和 native 路径各自适合什么场景

## 笔记模板

```md
# Day 7

## 客户端职责边界
- MgmtdClient:
- MetaClient:
- StorageClient:

## 两条访问路径
- FUSE:
- USRBIO:

## 闭环链路
- open:
- read:
- write:
- fsync:

## 还不清楚的问题
- Q1:
- Q2:
- Q3:
```
