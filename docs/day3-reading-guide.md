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
  simple_example 是 3FS 新服务的最小模板，用来展示一个服务从进程入口、Server、业务 RPC Service 到 fbs schema 的完整闭
  环。它的业务逻辑很少，echo 只是把请求里的 message 原样返回，所以重点不是业务，而是学习服务骨架和命名约定。
2. `SimpleExampleServer` 和 `SimpleExampleService` 分别代表什么层级？
  SimpleExampleServer 是进程内的服务容器层，它继承 net::Server，负责服务名、节点类型、配置、监听端口、ServiceGroup、
  mgmtd client、storage client，以及启动/停止生命周期。比如它定义 kName、kNodeType，配置 SimpleExampleSerde 和 Core
  两组服务，并在 beforeStart() 中注册 SimpleExampleService 和 CoreService，见 src/simple_example/service/
  Server.h:21、src/simple_example/service/Server.h:46、src/simple_example/service/Server.cc:50。

  SimpleExampleService 是业务 RPC 实现层，它继承 serde::ServiceWrapper<SimpleExampleService, SimpleExampleSerde>，表
  示“我实现了 fbs 中声明的 SimpleExampleSerde 服务”。它不管端口、线程池、进程启动、配置加载，只声明并实现具体 RPC 方
  法，例如 echo(SimpleExampleReq) -> SimpleExampleRsp，见 src/simple_example/service/Service.h:8、src/simple_example/
  service/Service.h:14、src/simple_example/service/Service.cc:13。
3. 业务 service 为什么不直接等于进程入口类？
  进程入口类负责的是应用生命周期：解析参数、加载 app/config、初始化公共组件、构造 Server、启动 Server，这些由
  TwoPhaseApplication<SimpleExampleServer> 完成，入口本身只有一行模板化启动逻辑，见 src/simple_example/main.cpp:7 和
  src/common/app/TwoPhaseApplication.h:45。业务 service 只应该处理 RPC 请求，否则一个 echo 方法实现会被迫知道配置来
  源、mgmtd、线程池、监听端口、停止流程等进程级细节。

  另外，一个进程通常不只暴露一个业务 service。SimpleExampleServer 同时注册了 SimpleExampleService 和 CoreService，说
  明 Server 是多个 RPC service 的宿主，而不是某一个业务方法集合本身，见 src/simple_example/service/Server.cc:50 和
  src/simple_example/service/Server.cc:51。
4. `src/fbs/simple_example` 中的定义和 `service/Service.h` 是怎样对应的？
    src/fbs/simple_example/SerdeService.h 定义协议面：SimpleExampleReq、SimpleExampleRsp，以及
  SERDE_SERVICE(SimpleExampleSerde, 0xF0) 下的 echo 方法，方法 id 是 1，请求/响应类型分别是 SimpleExampleReq 和
  SimpleExampleRsp，见 src/fbs/simple_example/SerdeService.h:8、src/fbs/simple_example/SerdeService.h:12、src/fbs/
  simple_example/SerdeService.h:16。service/Service.h 引入这个 schema，然后让 SimpleExampleService 继承
  ServiceWrapper<SimpleExampleService, SimpleExampleSerde>，并声明同名、同类型的 echo 方法。
    也就是说，fbs 侧决定“服务叫什么、服务 id 是多少、有哪些 RPC、请求响应类型是什么”，C++ Service 侧决定“这些 RPC 具体
  怎么执行”。最终 Server 的配置里写的是 "SimpleExampleSerde"，addSerdeService() 会按 Service::kServiceName 找到对应
  ServiceGroup，所以 schema 名、ServiceWrapper 绑定名、Server 配置名必须对齐，见 src/simple_example/service/
  Server.h:46 和 src/common/net/Server.h:42。
5. 为什么新增一个服务不只是复制 `src/simple_example`，还要补 `src/fbs/simple_example` 对应部分？
  因为 C++ 业务代码只是实现端，fbs/serde 定义才是 RPC 协议入口。没有对应的 fbs 定义，就没有请求/响应结构、服务名、服
  务 id、方法 id，也就无法生成或绑定客户端调用、服务注册和反射信息。

  Service.h 直接包含 fbs/simple_example/SerdeService.h，并依赖里面的 SimpleExampleSerde、SimpleExampleReq、
  SimpleExampleRsp。所以新增服务时必须同时复制并改名 src/<service> 和 src/fbs/<service>，这也正是 src/simple_example/
  README.md:1 的创建步骤所强调的。
6. `kNodeType` 在系统中可能被哪些逻辑使用？
  kNodeType 会被 ServerLauncher 从 Server::kNodeType 取出，用来向 mgmtd 拉取对应节点类型的配置模板，见 src/core/app/
  ServerLauncher.h:30、src/core/app/ServerLauncher.h:50、src/core/app/MgmtdClientFetcher.cc:21。正式服务里 mgmtd/
  meta/storage 分别设置成 MGMTD/META/STORAGE，而 simple_example 暂时是 CLIENT。

  它还可能影响节点注册、心跳、路由筛选、配置管理和管理命令。比如 NodeInfo 里保存 type 字段，selectNodeByType() 会按节
  点类型筛路由节点，NodeType 枚举包含 MGMTD/META/STORAGE/CLIENT/FUSE，见 src/fbs/mgmtd/NodeInfo.h:90、src/fbs/mgmtd/
  NodeInfo.h:98、src/fbs/mgmtd/MgmtdTypes.h:177。
7. 如果你要快速看懂一个新服务，先读哪 3 个文件最划算？
     第一读 main.cpp，看进程入口启动的是哪个 Server 类型；simple_example 里就是
  TwoPhaseApplication<SimpleExampleServer>()。第二读 service/Server.h，看 kName、kNodeType、Config、监听服务名、
  Launcher 和依赖组件。

  第三读业务 service/Service.h 或对应的 fbs service 定义；如果只能选 3 个，我会选 main.cpp、service/Server.h、src/
  fbs/<service>/...Service.h。读完这三个，再去 Service.cc 看方法实现，基本就能把“进程入口 -> Server 容器 -> RPC 协议
  -> 业务实现”的关系串起来。
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
