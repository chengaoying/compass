# Compass 项目架构梳理文档

> 版本：v1.2.0 | 更新时间：2026-03-16

---

## 一、项目概述

Compass 是一个开源的大数据任务诊断平台，自动采集 YARN、Spark、Flink 的日志和指标，利用启发式规则识别问题并生成调优建议。平台集成 ChatGPT，基于 Drain 算法聚合日志模板，生成智能诊断建议。

**核心能力：**
- 支持 Spark 离线批处理任务的自动诊断（数据倾斜、OOM、内存/CPU 浪费等 15+ 种异常）
- 支持 Flink 实时任务的资源优化诊断
- 覆盖调度层（任务失败、耗时异常、基线偏差）和引擎层（事件日志、容器日志解析）
- 支持 DolphinScheduler、Airflow 等调度系统
- ChatGPT 智能日志分析
- 一键诊断功能：输入 applicationId 即可获得完整诊断报告

---

## 二、工程结构

```
compass/
├── pom.xml                         # Maven 根 POM（多模块聚合）
│
├── task-common/                    # 公共库：领域模型、常量、工具类、OpenSearch/Redis 客户端
│   ├── com.oppo.cloud.common.constant/    # 枚举常量（AppCategoryEnum, LogType 等）
│   ├── com.oppo.cloud.common.domain/      # 领域模型
│   │   ├── cluster/                       #   YARN/Spark/Flink 集群数据模型
│   │   ├── eventlog/                      #   Spark EventLog 模型与检测配置
│   │   ├── flink/                         #   Flink 诊断模型
│   │   ├── job/                           #   Job/App/LogRecord 核心模型
│   │   └── opensearch/                    #   OpenSearch 文档模型
│   ├── com.oppo.cloud.model/              # MyBatis 生成的数据库实体
│   ├── com.oppo.cloud.mapper/             # MyBatis Mapper 接口
│   └── com.oppo.cloud.common.util/        # 工具类
│
├── task-mbg/                       # MyBatis Generator：自动生成数据库模型 & Mapper
│
├── task-syncer/                    # 数据同步服务（YARN/Spark/Flink 元数据采集）
│   ├── service/impl/              #   YarnSyncer, SparkSyncer, FlinkSyncer
│   └── 端口: 7074
│
├── task-metadata/                  # 元数据同步服务（调度系统用户/项目信息）
│   └── 端口: 7076
│
├── task-detect/                    # 异常检测服务（识别需要诊断的异常任务）
│   ├── detector/                  #   各类异常检测器
│   ├── service/                   #   TaskAppService, DelayedService
│   └── 端口: 7078
│
├── task-parser/                    # 日志解析服务（解析 HDFS 上的日志文件）
│   ├── service/consumer/          #   Kafka 消费者
│   ├── service/job/parser/        #   SparkEventLogParser, SparkExecutorLogParser
│   ├── service/job/detector/      #   日志模式匹配检测器
│   └── 端口: 7077
│
├── task-analyzer/                  # Spark EventLog 深度分析（Scala）
│   └── 实现 SparkListener 回放事件日志，提取 Stage/Task 级指标
│
├── task-portal/                    # Web 后端（REST API + 诊断报告服务）
│   ├── controller/                #   REST API 控制器
│   ├── service/diagnose/          #   诊断服务（runtime/resource/runerror）
│   ├── service/impl/              #   业务逻辑实现
│   └── 端口: 7075
│
├── task-flink/                     # Flink 实时诊断服务（可选）
│   └── 端口: 7079
│
├── task-gpt/                       # ChatGPT 日志分析服务
│   └── 端口: 7080
│
├── task-canal/                     # Canal CDC 集成（可选，调度系统数据库无法直连时使用）
│
├── task-ui/                        # 前端 UI（Vue 3 + Vite + Element Plus）
│   ├── src/pages/offline/         #   离线诊断页面
│   ├── src/pages/realtime/        #   Flink 实时诊断页面
│   └── src/pages/scheduler/       #   调度器页面
│
└── document/                       # 文档 & 部署配置
    ├── sql/                       #   MySQL / PostgreSQL 建表脚本
    ├── docker-compose/            #   Docker Compose 部署
    └── manual/                    #   使用手册
```

### 模块依赖关系

```
task-mbg ──(生成代码)──► task-common

task-common ◄──── 被所有服务模块依赖
       │
       ├──► task-syncer      （数据同步）
       ├──► task-metadata    （元数据同步）
       ├──► task-detect      （异常检测）
       ├──► task-parser      （日志解析，依赖 task-analyzer）
       ├──► task-analyzer    （Scala 事件日志分析）
       ├──► task-portal      （REST API）
       ├──► task-flink       （Flink 诊断）
       ├──► task-gpt         （GPT 分析）
       └──► task-canal       （Canal CDC）
```

---

## 三、技术架构

### 3.1 技术栈总览

| 层次 | 技术选型 | 版本 |
|------|----------|------|
| **语言** | Java | 1.8 (JDK 8) |
| **语言（分析器）** | Scala | 2.12.15 |
| **后端框架** | Spring Boot | 2.6.3 |
| **ORM** | MyBatis + MyBatis Generator | 2.2.2 |
| **关系数据库** | MySQL（主）/ PostgreSQL（备选） | 8.0 |
| **搜索引擎** | OpenSearch | 2.14.0 |
| **消息队列** | Apache Kafka + ZooKeeper | 3.7.0 / 3.9.2 |
| **缓存** | Redis | latest |
| **大数据** | Hadoop Client | 3.3.4 |
| **Spark 分析** | Spark Core + SQL | 3.2.2 |
| **前端框架** | Vue 3 + TypeScript + Vite | 3.2.41 / 3.1.8 |
| **UI 组件库** | Element Plus | 2.2.19 |
| **可视化** | ECharts | 5.4.0 |
| **认证** | JWT (jjwt 0.9.1) | - |
| **序列化** | FastJSON2 | 2.0.14 |
| **API 文档** | Swagger 2 (springfox 3.0.0) | - |
| **日志** | Log4j2 | 2.18.0 |
| **容器化** | Docker Compose | - |
| **Web 服务器** | Nginx | alpine |

### 3.2 三层架构

```
┌─────────────────────────────────────────────────────────┐
│                    接入层 (Data Sources)                  │
│  ┌──────────┐ ┌──────────┐ ┌─────────┐ ┌─────────────┐ │
│  │   YARN   │ │  Spark   │ │  Flink  │ │  Scheduler  │ │
│  │ Resource │ │ History  │ │ Metrics │ │ (DS/Airflow) │ │
│  │ Manager  │ │ Server   │ │         │ │  Database   │ │
│  └──────────┘ └──────────┘ └─────────┘ └─────────────┘ │
├─────────────────────────────────────────────────────────┤
│                    服务层 (Service Layer)                 │
│  ┌────────────┐ ┌──────────┐ ┌─────────┐ ┌───────────┐ │
│  │  Syncer    │ │ Metadata │ │ Detect  │ │  Parser   │ │
│  │ (元数据采集) │ │(用户同步) │ │(异常检测) │ │ (日志解析) │ │
│  └────────────┘ └──────────┘ └─────────┘ └───────────┘ │
│  ┌────────────┐ ┌──────────┐ ┌──────────┐              │
│  │  Portal    │ │  Flink   │ │   GPT    │              │
│  │  (API/UI)  │ │ (实时诊断) │ │ (智能分析) │              │
│  └────────────┘ └──────────┘ └──────────┘              │
├─────────────────────────────────────────────────────────┤
│                  基础设施层 (Infrastructure)               │
│  ┌─────────┐ ┌──────────┐ ┌───────┐ ┌──────┐ ┌──────┐ │
│  │  MySQL  │ │OpenSearch │ │ Kafka │ │Redis │ │ HDFS │ │
│  └─────────┘ └──────────┘ └───────┘ └──────┘ └──────┘ │
└─────────────────────────────────────────────────────────┘
```

### 3.3 服务间通信方式

| 通信路径 | 方式 | 说明 |
|----------|------|------|
| task-syncer → OpenSearch | REST API | 写入 YARN/Spark/Flink 应用元数据 |
| task-detect → Redis/Kafka | 发布消息 | 将 LogRecord 推入 Kafka `logRecord` Topic |
| Kafka → task-parser | 消费消息 | 消费 LogRecord 消息触发日志解析 |
| task-parser → HDFS | Hadoop Client | 读取 Spark EventLog、Executor 日志 |
| task-parser → OpenSearch | REST API | 写入诊断结果 |
| task-portal → MySQL/OpenSearch/Redis | 客户端 | 查询诊断数据、提交一键诊断 |
| Nginx → task-portal | HTTP 反向代理 | `/api/` 路径代理到 Portal |

---

## 四、功能架构

### 4.1 功能模块划分

```
Compass 平台
├── 数据采集
│   ├── YARN/Spark/Flink 应用元数据采集（task-syncer）
│   ├── 调度系统数据同步（task-syncer / task-canal）
│   └── 用户/项目元数据同步（task-metadata）
│
├── 异常诊断
│   ├── 调度层检测（task-detect）
│   │   ├── 任务失败检测
│   │   ├── 耗时异常检测（基线对比）
│   │   ├── 延迟任务检测
│   │   └── 首次失败告警
│   │
│   ├── Spark 引擎层诊断（task-parser + task-analyzer）
│   │   ├── 数据倾斜（Data Skew）
│   │   ├── 内存溢出（OOM / Memory Overflow）
│   │   ├── OOM 预警（Broadcast OOM）
│   │   ├── CPU 浪费（CPU Waste）
│   │   ├── 内存浪费（Memory Waste）
│   │   ├── 大表扫描（Large Table Scan）
│   │   ├── HDFS 卡顿（HDFS Stuck）
│   │   ├── 全局排序（Global Sort）
│   │   ├── 推测执行过多（Speculative Task）
│   │   ├── Job 时长异常
│   │   ├── Stage 时长异常
│   │   ├── Task 长尾
│   │   ├── Shuffle 失败
│   │   └── SQL 解析错误
│   │
│   ├── Flink 实时诊断（task-flink）
│   │   ├── 资源利用率分析
│   │   ├── 并行度优化建议
│   │   ├── TM/JM 内存优化
│   │   └── Slot 分配优化
│   │
│   └── 智能分析（task-gpt）
│       ├── Drain 日志模板聚合
│       └── ChatGPT 调优建议生成
│
├── 报表与可视化（task-portal + task-ui）
│   ├── CPU/内存使用趋势
│   ├── 异常分布统计
│   ├── 诊断报告详情
│   └── 任务执行详情
│
└── 系统管理
    ├── 用户认证（JWT）
    ├── 黑名单管理（Blocklist）
    └── 诊断规则配置（task_diagnosis_advice 表）
```

### 4.2 前端页面结构（task-ui）

```
pages/
├── index.vue              # 首页/仪表盘
├── login/                 # 登录页
├── offline/               # 离线诊断（Spark 批处理任务）
│   ├── list/              #   任务列表
│   ├── diagnosis/         #   一键诊断
│   └── report/            #   诊断报告
├── realtime/              # 实时诊断（Flink 流式任务）
└── scheduler/             # 调度器管理
```

### 4.3 REST API 接口

| Controller | 路径前缀 | 功能 |
|-----------|---------|------|
| UserController | `/api/v1/user` | 登录、用户信息、登出 |
| JobController | `/api/v1/job` | 任务列表、诊断报告、DAG 图 |
| OneClickDiagnosisController | `/api/v1/app` | 一键诊断（输入 applicationId） |
| ReportController | `/api/v1/report` | 统计报表、趋势图 |
| BlocklistController | `/api/v1/blocklist` | 黑名单 CRUD |
| FlinkTaskDiagnosisController | `/api/v1/flink` | Flink 诊断分页、报告 |

### 4.4 Portal 诊断服务架构

```
DiagnoseService (基础接口)
├── RunInfoService (判断展示哪些诊断项)
├── 运行时诊断 (runtime/):
│   ├── BigTableScanService          # 大表扫描
│   ├── DataSkewService              # 数据倾斜
│   ├── GlobalSortService            # 全局排序
│   ├── HdfsStuckService             # HDFS 卡顿
│   ├── JobDurationService           # Job 时长异常
│   ├── StageDurationService         # Stage 时长异常
│   ├── SpeculativeTaskService       # 推测执行
│   └── TaskLongTailService          # Task 长尾
├── 资源诊断 (resource/):
│   ├── CpuWasteService              # CPU 浪费
│   └── MemoryWasteService           # 内存浪费
└── 错误诊断 (runerror/):
    ├── MemoryOverflowService        # 内存溢出
    ├── SqlFailedService             # SQL 错误
    ├── ShuffleFailedService         # Shuffle 失败
    ├── OtherExceptionService        # 其他异常
    └── OOMWarnService               # OOM 预警
```

---

## 五、功能流程

### 5.1 数据采集与同步流程

```
┌──────────────────────────────────────────────────────────────┐
│                     数据采集流程                               │
│                                                              │
│  YARN ResourceManager ──(REST API)──► task-syncer            │
│  Spark History Server ──(REST API)──►    │                   │
│  Flink (via YARN) ──────(REST API)──►    │                   │
│                                          ▼                   │
│                              ┌──────────────────┐            │
│                              │  OpenSearch 索引   │            │
│                              │  yarn-app-*       │            │
│                              │  spark-app-*      │            │
│                              │  flink-app-*      │            │
│                              └──────────────────┘            │
│                                                              │
│  调度系统 DB ──(JDBC 轮询 / Canal CDC)──► task-syncer          │
│  (DolphinScheduler/Airflow)                │                 │
│                                            ▼                 │
│                                    MySQL (compass 库)         │
│                               (task, flow, project,          │
│                                task_application 表)           │
│                                                              │
│  调度系统 DB ──(JDBC)──► task-metadata                        │
│                              │                               │
│                              ▼                               │
│                     MySQL (user_info 表)                      │
└──────────────────────────────────────────────────────────────┘
```

### 5.2 异常检测与诊断流程

```
┌──────────────────────────────────────────────────────────────┐
│                     异常诊断流程                               │
│                                                              │
│  task-detect（定时调度）                                       │
│  ┌────────────────────────────────────────┐                   │
│  │  1. 从 MySQL 读取未处理的 TaskApplication  │                   │
│  │       │                                │                   │
│  │  2. 运行检测器链：                       │                   │
│  │     - JobDurationAbnormalDetector      │                   │
│  │     - DataSkewDetector                │                   │
│  │     - MemoryWasteDetector             │                   │
│  │     - CpuWasteDetector                │                   │
│  │     - ...（共 11 个检测器）              │                   │
│  │       │                                │                   │
│  │  3. 生成 LogRecord 消息                 │                   │
│  │       │                                │                   │
│  │  4. 推送到 Redis → Kafka (logRecord)    │                   │
│  └───────┼────────────────────────────────┘                   │
│          │                                                    │
│          ▼                                                    │
│  task-parser（Kafka Consumer）                                │
│  ┌────────────────────────────────────────┐                   │
│  │  1. 消费 LogRecord 消息                 │                   │
│  │       │                                │                   │
│  │  2. 根据 LogType 选择解析器：            │                   │
│  │     - SparkEventLogParserJob           │                   │
│  │       → task-analyzer (Scala SparkListener)│                │
│  │       → 回放事件日志，提取 Stage/Task 指标  │                   │
│  │       → 运行引擎层检测器                  │                   │
│  │     - SparkExecutorLogParserJob        │                   │
│  │       → 读取 HDFS 上的 Executor/Driver 日志│                │
│  │       → 模式匹配（OOM、Shuffle 失败等）    │                   │
│  │       │                                │                   │
│  │  3. 写入诊断结果到 OpenSearch            │                   │
│  │     (log-summary-*, task-app-* 索引)    │                   │
│  └────────────────────────────────────────┘                   │
└──────────────────────────────────────────────────────────────┘
```

### 5.3 一键诊断流程

```
用户输入 applicationId
       │
       ▼
task-portal: OneClickDiagnosisServiceImpl.diagnose()
       │
       ├──→ 1. 查 MySQL (task_application) + OpenSearch (yarn-app, spark-app)
       │       → 构建 TaskApp 对象
       │
       ├──→ 2. 检查 Redis 缓存（是否正在诊断/已完成）
       │       → 有进度：返回 PROCESSING/SUCCEED/FAILED
       │
       ├──→ 3. 首次诊断：创建 LogRecord → 推送 Redis/Kafka
       │       → 设置进度追踪 (Redis TTL 24h)
       │       → 返回 PROCESSING 状态
       │
       ▼
task-parser 消费消息 → 解析日志 → 写入 OpenSearch
       │
       ▼
前端轮询 /api/v1/app/diagnose → 获取进度/结果
```

### 5.4 用户查询流程

```
用户浏览器 ──► Nginx (:80)
                │
                ├── 静态资源 → /usr/share/nginx/html (Vue SPA)
                │
                └── /api/* → 反向代理 → task-portal (:7075)
                                           │
                              ┌────────────┼────────────┐
                              ▼            ▼            ▼
                           MySQL      OpenSearch      Redis
                          (元数据)     (诊断结果)      (缓存)
```

---

## 六、部署架构

### 6.1 服务端口映射

| 服务 | 端口 | 说明 |
|------|------|------|
| task-syncer | 7074 | YARN/Spark/Flink 数据同步 |
| task-portal | 7075 | REST API 后端 |
| task-metadata | 7076 | 元数据同步 |
| task-parser | 7077 | 日志解析 |
| task-detect | 7078 | 异常检测 |
| task-flink | 7079 | Flink 诊断 |
| task-gpt | 7080 | ChatGPT 分析 |
| task-ui (Nginx) | 80 | 前端 Web |

### 6.2 基础设施依赖

| 组件 | 版本 | 用途 | 端口 |
|------|------|------|------|
| MySQL | 8.0 | 关系数据库 | 3306 |
| Redis | latest | 缓存 & 消息中转 | 6379 |
| OpenSearch | 2.14.0 | 搜索引擎 & 诊断数据存储 | 9200 |
| OpenSearch Dashboards | 2.14.0 | 可视化面板 | 5601 |
| Kafka | 3.7.0 | 服务间消息队列 | 9092 |
| ZooKeeper | 3.9.2 | Kafka 协调 | 2181 |
| HDFS | 依赖已有集群 | 日志文件存储 | 8020 |
| YARN RM | 依赖已有集群 | 应用数据 REST API | 8088 |
| Spark HS | 依赖已有集群 | Spark 应用元数据 | 18089 |

### 6.3 Docker Compose 部署拓扑

```
┌─────────────────────────────────────────────────────────┐
│                    Docker Host                          │
│                                                         │
│  ┌─────────┐  ┌─────────┐  ┌──────────┐  ┌──────────┐ │
│  │  MySQL   │  │  Redis  │  │OpenSearch│  │  Kafka   │ │
│  │  :3306   │  │  :6379  │  │  :9200   │  │  :9092   │ │
│  └─────────┘  └─────────┘  └──────────┘  └──────────┘ │
│  ┌──────────┐                              ┌──────────┐│
│  │ZooKeeper │                              │OS Dashbd ││
│  │  :2181   │                              │  :5601   ││
│  └──────────┘                              └──────────┘│
│                                                         │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌───────────┐ │
│  │ Portal   │ │ Syncer   │ │ Detect   │ │  Parser   │ │
│  │ :7075    │ │ :7074    │ │ :7078    │ │  :7077    │ │
│  └──────────┘ └──────────┘ └──────────┘ └───────────┘ │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐               │
│  │ Metadata │ │  Flink   │ │ UI/Nginx │               │
│  │ :7076    │ │  :7079   │ │  :80     │               │
│  └──────────┘ └──────────┘ └──────────┘               │
│                                                         │
│  外部依赖：YARN RM, Spark HS, HDFS, Prometheus         │
└─────────────────────────────────────────────────────────┘
```

### 6.4 OpenSearch 索引设计

| 索引模式 | 用途 | 写入方 |
|---------|------|--------|
| `yarn-app-*` | YARN 应用元数据 | task-syncer |
| `spark-app-*` | Spark 应用元数据 | task-syncer |
| `flink-app-*` | Flink 应用元数据 | task-syncer |
| `task-app-*` | 诊断应用数据 | task-detect / task-parser |
| `job-analysis-*` | Job 级诊断分析 | task-detect |
| `log-summary-*` | 日志解析摘要 | task-parser |
| `flink-report-*` | Flink 诊断报告 | task-flink |

### 6.5 Kafka Topic 设计

| Topic | 生产者 | 消费者 | 数据内容 |
|-------|--------|--------|---------|
| `logRecord` | task-detect / task-portal | task-parser | LogRecord（日志解析任务） |

---

## 七、数据库设计

### 核心表结构

| 表名 | 用途 | 关键字段 |
|------|------|---------|
| user_info | 用户信息 | username, password, is_admin |
| project | 项目元数据 | project_name, user_id |
| flow | 工作流定义 | flow_name, project_name |
| task | 任务定义 | task_name, flow_name, project_name |
| task_application | 引擎应用映射 | application_id, task_name, execute_time |
| task_diagnosis_advice | 诊断规则配置 | log_type, action, category, rule |
| blocklist | 诊断黑名单 | component, value |

---

## 八、存在的问题及优化建议

### 8.1 架构层面

| # | 问题 | 严重度 | 影响 | 优化建议 |
|---|------|--------|------|---------|
| 1 | **微服务拆分过细** — 7 个 Java 服务 + 1 个前端，但大部分服务通信路径是线性的（syncer → detect → parser），增加了部署和运维复杂度 | **高** | 资源浪费（每个服务独立 JVM），排障跨多个服务日志 | 考虑合并 task-detect + task-parser 为一个服务；合并 task-syncer + task-metadata |
| 2 | **task-common 职责过大** — 承载了所有领域模型、常量、MyBatis Mapper、工具类，所有模块重度依赖 | **中** | 任何模型变更触发全量编译；模块边界模糊 | 按领域拆分：`compass-model-spark`、`compass-model-flink`、`compass-model-scheduler` |
| 3 | **task-portal 职责过载** — 同时负责 REST API、Spark 诊断报告渲染、一键诊断提交、OpenSearch 查询初始化 | **中** | 单点故障，难以独立扩缩容 | 将诊断报告生成逻辑抽离为独立服务或库 |
| 4 | **task-detect 和 task-parser 数据交互依赖 Redis + Kafka** — 检测结果先写 Redis 再转 Kafka，增加了不必要的中间环节 | **中** | Redis 单点故障影响整个诊断链路 | 直接使用 Kafka 传递 LogRecord，Redis 仅用于缓存和进度追踪 |

### 8.2 代码质量与可靠性

| # | 问题 | 严重度 | 影响 | 优化建议 |
|---|------|--------|------|---------|
| 5 | **事务管理几乎缺失** — 全代码库仅 2 处 `@Transactional`；task-syncer 多表写入无事务，Kafka commitAsync 不依赖 DB 成功 | **高** | 数据库部分更新导致不一致；消息已确认但写入失败 | 关键写操作包裹 `@Transactional`；Kafka 手动 commit 须在 DB 成功后执行 |
| 6 | **错误处理反模式** — 多处 `e.printStackTrace()`（DateUtil、HttpUtil 等）；ChatGPT 调用失败静默丢弃 | **高** | 容器日志丢失堆栈；GPT 分析结果缺失无告警 | 全部替换为 `log.error()`；GPT 调用加重试 + DLQ |
| 7 | **测试覆盖率极低** — ~500 个 Java 源文件仅 ~22 个测试文件，核心检测逻辑无测试 | **中** | 重构和功能变更风险高 | 优先为检测器和解析器编写单元测试，目标覆盖率 > 60% |
| 8 | **双 JWT 库共存** — 同时依赖 java-jwt 和 jjwt | **低** | 维护混乱，版本冲突风险 | 统一使用一个 JWT 库 |
| 9 | **硬编码配置** — Kafka Topic 名称、线程池参数散落在代码中 | **低** | 环境切换困难 | 集中到 application.yml |

### 8.3 技术栈问题

| # | 问题 | 严重度 | 影响 | 优化建议 |
|---|------|--------|------|---------|
| 10 | **Java 8 已 EOL** — JDK 8 已不再获得免费安全更新 | **高** | 安全风险；无法使用 Java 11+ 新特性（Records、HttpClient 等） | 升级至 Java 17 LTS + Spring Boot 3.x |
| 11 | **Spring Boot 2.6.3 已停止维护** — Spring Boot 2.x 于 2023 年 11 月 EOL | **高** | 无安全补丁 | 升级至 Spring Boot 3.2+ |
| 12 | **Swagger 2 (springfox) 不兼容 Spring Boot 3** — springfox 项目已停止维护 | **中** | 升级 Spring Boot 时必须迁移 | 迁移至 SpringDoc OpenAPI 3.x |
| 13 | **OpenSearch Client 版本滞后** — 使用 `opensearch-rest-high-level-client:2.1.0`，而 OpenSearch 服务端为 2.14.0 | **中** | 新功能不可用，可能存在兼容性问题 | 升级至 OpenSearch Java Client 2.x |

### 8.4 安全

| # | 问题 | 严重度 | 影响 | 优化建议 |
|---|------|--------|------|---------|
| 14 | **Session Cookie 未设 HttpOnly** — application.yml 中 `http-only: false` | **高** | XSS 攻击可窃取会话令牌 | 改为 `http-only: true`，添加 `secure` 标志 |
| 15 | **Docker Compose 硬编码凭证** — 数据库密码明文写在 compose 文件中 | **中** | 安全隐患 | 使用 Docker Secrets 或 `.env` 文件 |
| 16 | **Swagger UI 生产环境暴露** — 无环境限制 | **低** | API 接口暴露 | 仅在 dev/test Profile 下启用 |
| 17 | **CORS 配置过于宽松** — `allowedOrigins("*")` | **中** | 跨站请求伪造风险 | 限制为实际前端域名 |

### 8.5 部署与运维

| # | 问题 | 严重度 | 影响 | 优化建议 |
|---|------|--------|------|---------|
| 18 | **无健康检查机制** — 微服务无统一健康检查 | **中** | 服务异常难以快速定位 | 启用 Spring Boot Actuator `/health`，接入 Prometheus + Grafana |
| 19 | **仅支持 Docker Compose 部署** — 无 Kubernetes/Helm 支持 | **中** | 不适合大规模生产环境 | 提供 Helm Chart |
| 20 | **无 CI/CD 流水线** — 无 GitHub Actions 或其他 CI 配置 | **中** | 代码质量无自动化保障 | 添加 CI：编译 → 测试 → 镜像构建 |
| 21 | **OpenSearch 索引无生命周期管理** — 无自动滚动和清理策略 | **中** | 长期运行后存储膨胀 | 配置 ISM 策略 |
| 22 | **无分布式追踪** — 跨服务消息流无端到端追踪 | **低** | 排障困难 | 引入 OpenTelemetry |

---

## 九、优化路线图（建议）

### 紧急（1-2 周）— 安全 & 数据可靠性
1. **修复 Cookie HttpOnly** — `http-only: true` (#14)
2. **修复 CORS 配置** — 限制允许的源 (#17)
3. **修复 `printStackTrace`** — 全局替换为 `log.error()` (#6)
4. **GPT 调用加重试** — 失败不再静默丢弃 (#6)

### 短期（1-2 个月）— 可靠性 & 质量
5. **增加事务管理** — 关键写操作包裹 `@Transactional` (#5)
6. **补充核心测试** — 检测器和解析器单元测试 (#7)
7. **统一 JWT 库** + 迁移 Swagger → SpringDoc (#8, #12)
8. **添加 CI 流水线** (#20)
9. **启用 Actuator 健康检查** (#18)

### 中期（3-6 个月）— 技术升级
10. **升级 Java 17 + Spring Boot 3.2** (#10, #11)
11. **升级 OpenSearch Client** (#13)
12. **合并 detect + parser 为单服务** — 减少部署复杂度 (#1)
13. **配置 OpenSearch ISM 索引生命周期** (#21)
14. **提供 Helm Chart** (#19)

### 长期（6-12 个月）— 架构演进
15. **拆分 task-common** — 按领域分包 (#2)
16. **Kafka 去 ZooKeeper** — 迁移至 KRaft 模式
17. **引入 OpenTelemetry** — 分布式追踪 (#22)
18. **支持更多引擎** — Presto/Trino、Hive on Tez
19. **多租户 + RBAC 细粒度权限**
