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
    TwoPhaseApplication 解决的是“所有服务进程启动流程重复”的问题：解析三类配置参数、初始化 launcher、加载 AppInfo、加载并渲染服务配置、初始化公共组件、创建 server、setup、start、stop。服务复用它，是因为每个服务只需要提供Server::Config/CommonConfig/Launcher/... 这些类型和自身的 beforeStart 逻辑，公共启动骨架不用重复写
2. `ServerLauncher` 负责的是服务生命周期的哪一段？
    ServerLauncher 负责的是 server 真正启动前后的 bootstrap 阶段，不负责业务 RPC 的具体实现。它加载 app_cfg 和 launcher_cfg，启动 IBManager，创建 RemoteConfigFetcher，再通过 fetcher 获取配置模板、补全 AppInfo，最后调用 server.start(appInfo) 或 fetcher 自定义的 startServer。
3. `app_cfg`、`launcher_cfg`、`cfg` 这几类配置，角色分别是什么？
    app_cfg 是单进程自身身份配置，典型字段是 node_id，用于构造基础 AppInfo。launcher_cfg 是启动器配置，包含cluster_id、IB 设备、client、mgmtd client 等，用来连接管理面并拉取远端配置。cfg 是服务运行配置，也就是 TwoPhaseApplication::Config { common, server }，其中 common 管日志/监控/内存，server 管服务自己的 net::Server::Config 和业务配置；它可以来自本地文件、默认配置或 launcher 从 mgmtd 拉到的模板。
4. `net::Server::Config` 中的 `groups` 是干什么的？
    net::Server::Config::groups 是 service group 列表，每个 group 对应一套 ServiceGroup 配置：服务名集合、网络类型、listener、IOWorker、Processor，以及是否使用独立线程池。Server 构造时会按 groups_length() 创建多个 ServiceGroup，setup/start/stop 也都是逐 group 执行，所以它是 RPC 服务挂载、监听地址和线程资源隔离的基本单元。
5. 为什么很多服务都把业务 RPC 和 `Core` service 分在两个 group 里？
    业务 RPC 和 Core service 分 group，是为了把数据面和控制面隔开。业务服务通常走 RDMA 和主线程池，Core 默认走 TCP、独立线程池，用于 echo/getConfig/renderConfig/hotUpdateConfig/shutdown 等管理操作，避免业务流量拥塞时管理入口也被拖住。
6. `serde::ServiceWrapper` 在这个项目中的作用是什么？
    serde::ServiceWrapper 是服务实现类和 generated service 描述之间的桥。它把 Service<void>::kServiceName/kServiceID暴露给运行时，让 Server::addSerdeService 能按服务名找到 group，让 Services::addService 能按 service id 建dispatch 表。它还通过反射接口把 SERDE_SERVICE_METHOD 生成的 method 元信息交给 MethodExtractor，最终把 method id映射到具体 C++ 成员函数。
7. 如果你新加一个服务，最小需要补哪些类型定义和入口？
    新加一个服务，最小需要补：请求/响应结构和 SERDE_SERVICE/SERDE_SERVICE_METHOD 定义，服务实现类继承serde::ServiceWrapper<Impl, ServiceBase>，并实现对应 RPC 方法。还需要一个派生自 net::Server 的 server 类型，定义kName/kNodeType/CommonConfig/AppConfig/LauncherConfig/RemoteConfigFetcher/Launcher/Config，在 beforeStart 里addSerdeService，再补一个 main 用 TwoPhaseApplication<YourServer>().run(argc, argv) 和对应 CMake 目标。

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
