# 3FS Day 2 Reading Guide

## 目标

今天的目标是吃透 3FS 服务进程的公共启动骨架，理解一个服务如何被创建、加载配置、启动网络层并挂载 RPC。

到今天结束时，你应该能用伪代码讲清楚这条主链路：

`main -> TwoPhaseApplication -> Launcher init -> config init -> server setup -> server start`

## 今日学习路径

建议严格按下面顺序执行：

1. 读 `src/CMakeLists.txt`，确认顶层模块装配关系
2. 读 `src/common/app/TwoPhaseApplication.h`
3. 读 `src/core/app/ServerLauncher.h`
4. 读 `src/common/net/Server.h`
5. 读 `src/common/serde/Service.h`
6. 读 `src/core/service/CoreService.h`

今天不要提前钻进 `common/utils/`，先把骨架吃透。

## 分阶段任务

### 第一阶段：看模块装配和进程骨架

建议用时：

- 40 到 50 分钟

任务：

- [ ] 读完 `src/CMakeLists.txt`
- [ ] 读完 `src/common/app/TwoPhaseApplication.h`
- [ ] 写出“一个服务是如何被统一包装起来的”

重点关注：

- 顶层有哪些模块被装配进来
- `TwoPhaseApplication` 解决了什么共性问题
- 配置和 server 对象在生命周期中出现的顺序

阶段产出：

- 一份服务启动主骨架

### 第二阶段：看 launcher 和 server

建议用时：

- 45 到 60 分钟

任务：

- [ ] 读完 `src/core/app/ServerLauncher.h`
- [ ] 读完 `src/common/net/Server.h`
- [ ] 写出 `Launcher` 和 `Server` 的职责边界

重点关注：

- `app_cfg`、`launcher_cfg`、`cfg` 的角色
- 配置模板是怎么被加载进来的
- `net::Server` 如何组织线程池和 service groups

阶段产出：

- 一段对 `Launcher` / `Server` 分工的解释

### 第三阶段：看 RPC 抽象

建议用时：

- 30 到 40 分钟

任务：

- [ ] 读完 `src/common/serde/Service.h`
- [ ] 读完 `src/core/service/CoreService.h`
- [ ] 解释 `serde::ServiceWrapper` 和 `CoreService` 在体系里的位置

重点关注：

- `serde` 如何定义 service 和 method
- 为什么很多服务都带一个 `Core` service
- `CoreService` 在整个框架中扮演什么角色

阶段产出：

- 一段对 RPC 挂载方式的总结

## 今日练习题

1. `TwoPhaseApplication` 解决了什么问题，为什么服务都复用它？
2. `ServerLauncher` 负责的是服务生命周期的哪一段？
3. `app_cfg`、`launcher_cfg`、`cfg` 这几类配置，角色分别是什么？
4. `net::Server::Config` 中的 `groups` 是干什么的？
5. 为什么很多服务都把业务 RPC 和 `Core` service 分在两个 group 里？
6. `serde::ServiceWrapper` 在这个项目中的作用是什么？
7. 如果你新加一个服务，最小需要补哪些类型定义和入口？
8. 为什么这一天不建议先深入 `common/utils/`？

答题要求：

- 每题至少写 2 句
- 第 1、3、4、6 题要重点认真写

## 进入 Day 3 前的门槛

满足下面 4 项，再进入 Day 3：

- [ ] 我已经读完今天的 6 个核心文件
- [ ] 我已经独立回答完 8 道练习题
- [ ] 我已经写出统一启动骨架
- [ ] 我已经能解释 `Launcher`、`Server`、`serde` 三者的关系

## 笔记模板

```md
# Day 2

## 启动骨架
- main:
- TwoPhaseApplication:
- ServerLauncher:
- net::Server:

## 关键概念
- app_cfg:
- launcher_cfg:
- cfg:
- groups:
- CoreService:

## 主调用链
- 服务启动链:
- RPC 挂载链:

## 还不清楚的问题
- Q1:
- Q2:
- Q3:
```
