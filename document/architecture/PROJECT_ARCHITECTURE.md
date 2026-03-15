# Compass 项目架构梳理文档

> 版本：v1.1.2 | 更新时间：2026-03-15

---

## 一、项目概述

Compass 是一个开源的大数据计算引擎与任务调度诊断平台。它自动采集日志和指标数据，利用启发式规则识别问题，并提供调优建议。平台集成 ChatGPT，基于 Drain 算法聚合日志模板，生成智能诊断建议。

**核心能力：**
- 支持 85+ 种异常类别的自动诊断
- 覆盖调度层（任务失败、耗时异常）和引擎层（数据倾斜、OOM、Shuffle 失败等）
- 支持 Spark、Flink、MapReduce 多种计算引擎
- 支持 DolphinScheduler、Airflow 等多种调度系统
- ChatGPT 智能日志分析

---

## 二、工程结构

```
compass/
├── pom.xml                         # Maven 根 POM（多模块聚合）
│
├── task-common/                    # 公共库：领域模型、工具类、OpenSearch/Redis 客户端
├── task-mbg/                       # MyBatis Generator：数据库模型 & Mapper
│
├── task-analyzer/                  # 【核心服务】异常检测 + 日志解析（合并自 task-detect + task-parser）
├── task-collector/                 # 【核心服务】元数据采集（合并自 task-application + task-metadata）
├── task-syncer/                    # 【核心服务】调度系统数据同步与标准化
├── task-portal/                    # 【核心服务】REST API 后端 + Web 服务
├── task-gpt/                       # 【核心服务】ChatGPT 日志分析服务
│
├── task-flink-core/                # Flink 诊断核心模型与指标
├── task-flink/                     # Flink 实时诊断服务（可选）
│
├── task-canal/                     # Canal CDC 集成（可选，当无法直连调度系统数据库时使用）
├── task-canal-adapter/             # Canal 适配器
├── task-flume-plugin/              # Flume 日志采集插件
│
├── task-ui/                        # 前端 UI（Vue 3 + Vite + Element Plus）
├── task-assembly/                  # 分发打包 & 启动脚本
│
├── docker/                         # Docker 部署配置
│   └── playground/                 # Docker Compose 全栈部署
│       ├── compose.yml
│       ├── conf/                   # Hadoop/Spark/DolphinScheduler 配置模板
│       ├── dockerfiles/            # 各服务 Dockerfile
│       └── script/                 # 初始化 SQL 脚本
│
└── document/                       # 文档 & SQL Schema
    ├── sql/                        # PostgreSQL / MySQL 建表脚本
    └── architecture/               # 架构文档（本目录）
```

### 模块依赖关系

```
task-mbg ──────────────────┐
                           ▼
task-common ◄──── 被所有服务模块依赖
                           │
     ┌─────────────────────┼─────────────────────┐
     ▼                     ▼                     ▼
task-analyzer         task-collector         task-syncer
(含 task-parser 包)                              │
     │                     │                     │
     ▼                     ▼                     ▼
task-portal ◄──────────────┘─────────────────────┘
  (含 task-flink-core)
     │
     ▼
task-gpt（独立消费 Kafka）
```

---

## 三、技术架构

### 3.1 技术栈总览

| 层次 | 技术选型 | 版本 |
|------|----------|------|
| **后端框架** | Spring Boot | 3.2.4 |
| **ORM** | MyBatis + MyBatis-Plus | 3.5.11 / 3.5.6 |
| **关系数据库** | PostgreSQL（主推）/ MySQL | 10.0+ / 5.7+ |
| **搜索引擎** | OpenSearch（替代 Elasticsearch） | 2.13.0 |
| **消息队列** | Apache Kafka（KRaft 模式，去 ZooKeeper） | 3.4.0 |
| **缓存** | Redis（单节点，去集群化） | 7.2.1 |
| **大数据集成** | Hadoop Client | 3.3.4 |
| **前端框架** | Vue 3 + Vite | - |
| **前端 UI 库** | Element Plus | - |
| **可视化** | ECharts | - |
| **AI 集成** | ChatGPT Java SDK + Drain 算法 | 1.1.3 |
| **认证** | JWT（java-jwt + jjwt） | 4.2.2 / 0.9.0 |
| **监控** | Micrometer Prometheus + Actuator | 1.12.4 |
| **连接池** | Druid | 1.2.15 |
| **构建工具** | Maven + pnpm | 3.6.0+ / 7.8.0 |
| **容器化** | Docker Compose | - |

### 3.2 三层架构

```
┌─────────────────────────────────────────────────────────┐
│                    接入层 (Docking Layer)                  │
│  ┌──────────┐ ┌──────────┐ ┌─────────┐ ┌─────────────┐  │
│  │   YARN   │ │  Spark   │ │  Flink  │ │  Scheduler  │  │
│  │ Resource │ │ History  │ │ Metrics │ │ (DS/Airflow) │  │
│  │ Manager  │ │ Server   │ │         │ │  Database   │  │
│  └──────────┘ └──────────┘ └─────────┘ └─────────────┘  │
├─────────────────────────────────────────────────────────┤
│                    服务层 (Service Layer)                  │
│  ┌────────────┐ ┌──────────┐ ┌─────────┐ ┌───────────┐  │
│  │ Collector  │ │ Syncer   │ │Analyzer │ │  Portal   │  │
│  │ (元数据采集) │ │(数据同步) │ │(异常检测) │ │ (API/UI) │  │
│  └────────────┘ └──────────┘ └─────────┘ └───────────┘  │
│  ┌────────────┐ ┌──────────┐                             │
│  │   GPT      │ │  Flink   │                             │
│  │ (智能分析)  │ │ (实时诊断) │                             │
│  └────────────┘ └──────────┘                             │
├─────────────────────────────────────────────────────────┤
│                  基础设施层 (Infrastructure)                │
│  ┌─────────┐ ┌──────────┐ ┌───────┐ ┌──────────────┐    │
│  │PostgreSQL│ │OpenSearch │ │ Kafka │ │    Redis    │    │
│  └─────────┘ └──────────┘ └───────┘ └──────────────┘    │
└─────────────────────────────────────────────────────────┘
```

---

## 四、功能架构

### 4.1 功能模块划分

```
Compass 平台
├── 数据接入
│   ├── 调度系统同步（DolphinScheduler 2.x/3.x、Airflow 2.x）
│   ├── CDC 数据捕获（Canal → Kafka）
│   └── YARN/Spark/Flink 元数据采集
│
├── 异常诊断
│   ├── 调度层诊断
│   │   ├── 任务失败检测
│   │   ├── 耗时异常检测（基线对比）
│   │   ├── 长尾任务检测
│   │   ├── 首次失败告警
│   │   ├── 任务持续时间异常
│   │   └── 任务基线偏差
│   │
│   ├── Spark/MR 引擎诊断
│   │   ├── 数据倾斜（Data Skew）
│   │   ├── 内存溢出（OOM）
│   │   ├── CPU 浪费（CPU Waste）
│   │   ├── 内存浪费（Memory Waste）
│   │   ├── 大表扫描（Large Table Scan）
│   │   ├── HDFS 卡顿（HDFS Stuck）
│   │   ├── 全局排序（Global Sort）
│   │   ├── 推测执行（Speculative Task）
│   │   ├── Job 时长异常
│   │   ├── Stage 时长异常
│   │   ├── Task 时长异常
│   │   └── GC 分析
│   │
│   ├── Flink 实时诊断
│   │   ├── 资源利用率分析
│   │   ├── 反压检测（Back Pressure）
│   │   ├── 算子性能分析
│   │   └── 数据分布分析
│   │
│   └── 智能分析（GPT）
│       ├── Drain 日志模板聚合
│       ├── ChatGPT 调优建议生成
│       └── LRU 缓存优化
│
├── 报表与可视化
│   ├── 诊断报告（离线/实时）
│   ├── 趋势分析图表
│   ├── 异常分布统计
│   └── 任务执行详情
│
└── 系统管理
    ├── 用户认证（JWT）
    ├── 黑白名单管理
    ├── 诊断规则配置
    └── 多调度系统支持
```

### 4.2 前端页面结构（task-ui）

```
pages/
├── index.vue              # 首页/仪表盘
├── login/                 # 登录页
├── offline/               # 离线诊断（Spark/MR 批处理任务）
└── realtime/              # 实时诊断（Flink 流式任务）
```

### 4.3 REST API 接口

| Controller | 路径前缀 | 功能 |
|-----------|---------|------|
| UserController | /compass/user | 用户登录、注册、管理 |
| ReportController | /compass/report | 诊断报告、统计数据 |
| JobController | /compass/job | 工作流/任务级分析 |
| AppController | /compass/app | Spark/MR Application 诊断 |
| FlinkTaskDiagnosisController | /compass/flink | Flink 任务诊断 |
| FlinkMetadataController | /compass/flink/metadata | Flink 元数据管理 |
| BlocklistController | /compass/blocklist | 诊断黑名单管理 |
| LogRecordController | /compass/log | 日志记录查询 |

---

## 五、功能流程

### 5.1 数据采集与同步流程

```
┌──────────────────────────────────────────────────────────────┐
│                     数据采集流程                                │
│                                                              │
│  调度系统 DB                                                   │
│  (DolphinScheduler/Airflow)                                  │
│       │                                                      │
│       ├──→ [Canal CDC] ──→ Kafka[mysqldata]                  │
│       │    (可选)                    │                        │
│       │                             ▼                        │
│       │                      task-syncer                     │
│       │                   (数据标准化)                         │
│       │                      │      │                        │
│       │                      ▼      ▼                        │
│       │               PostgreSQL  Kafka[task-instance]        │
│       │                                   │                  │
│       │                                   ▼                  │
│       │                            task-collector             │
│       │                         (YARN/Spark 元数据采集)         │
│       │                              │                       │
│       │                              ▼                       │
│       └──→ [直连 DB] ──→ YARN RM / Spark HistoryServer       │
│                              │                               │
│                              ▼                               │
│                         PostgreSQL                           │
│                    (task_application 表)                      │
└──────────────────────────────────────────────────────────────┘
```

### 5.2 异常诊断流程

```
┌──────────────────────────────────────────────────────────────┐
│                     异常诊断流程                                │
│                                                              │
│  Kafka[task-instance]                                        │
│       │                                                      │
│       ▼                                                      │
│  task-analyzer                                               │
│  ┌────────────────────────────────────────┐                  │
│  │  1. Kafka Consumer 消费任务实例消息       │                  │
│  │       │                                │                  │
│  │       ▼                                │                  │
│  │  2. 调度层检测（6种检测器）               │                  │
│  │     - TaskFailedDetector               │                  │
│  │     - AbnormalDurationDetector         │                  │
│  │     - LongTailDetector                 │                  │
│  │     - FirstFailureDetector             │                  │
│  │     - ...                              │                  │
│  │       │                                │                  │
│  │       ▼                                │                  │
│  │  3. AnalyzerBridge（进程内直调）         │                  │
│  │     (替代原 Redis 队列中转)              │                  │
│  │       │                                │                  │
│  │       ▼                                │                  │
│  │  4. 引擎层日志解析（JobManager）         │                  │
│  │     - EventLog 解析                    │                  │
│  │     - Executor 日志解析                 │                  │
│  │     - 数据倾斜/OOM/CPU浪费等检测         │                  │
│  │       │                                │                  │
│  └───────┼────────────────────────────────┘                  │
│          │                                                   │
│          ├──→ OpenSearch（写入诊断结果索引）                     │
│          │                                                   │
│          └──→ Kafka[task-gpt]                                │
│                    │                                         │
│                    ▼                                         │
│              task-gpt                                        │
│          ┌────────────────────┐                               │
│          │ Drain 日志模板聚合   │                               │
│          │       │            │                               │
│          │       ▼            │                               │
│          │ ChatGPT API 调用   │                               │
│          │       │            │                               │
│          │       ▼            │                               │
│          │ LRU 缓存 + 存储     │                               │
│          └────────────────────┘                               │
└──────────────────────────────────────────────────────────────┘
```

### 5.3 用户查询流程

```
用户浏览器 ──→ task-ui (Vue 3 SPA)
                  │
                  ▼
              task-portal (REST API)
                  │
        ┌─────────┼─────────┐
        ▼         ▼         ▼
   PostgreSQL  OpenSearch  Redis
   (元数据)    (诊断结果)   (缓存)
```

---

## 六、部署架构

### 6.1 服务端口映射

| 服务 | 端口 | 说明 |
|------|------|------|
| task-analyzer | 7071 | 异常检测 + 日志解析 |
| task-collector | 7072 | 元数据采集 |
| task-syncer | 7073 | 数据同步 |
| task-gpt | 7074 | GPT 智能分析 |
| task-portal | 7075 | REST API（`/compass` 上下文） |
| task-ui | 3333 | 前端开发服务器 |

### 6.2 基础设施依赖

| 组件 | 用途 | 部署要求 |
|------|------|---------|
| PostgreSQL | 元数据存储 | 单节点 |
| Kafka (KRaft) | 服务间消息传递 | 单节点/集群 |
| Redis | Portal 查询缓存 | 单节点（已去集群化） |
| OpenSearch | 诊断结果索引 | 单节点/集群 |
| HDFS | 日志文件存储 | 依赖已有集群 |

### 6.3 Kafka Topic 设计

| Topic | 生产者 | 消费者 | 数据内容 |
|-------|--------|--------|---------|
| mysqldata | Canal CDC | task-syncer | 调度系统数据库变更事件 |
| task-instance | task-syncer | task-analyzer | 任务实例执行记录 |
| task-application | task-syncer | task-collector | 应用发现事件 |
| exception-log | task-analyzer | task-portal | 异常日志记录 |
| task-gpt | task-analyzer | task-gpt | 待分析的日志模板 |

### 6.4 Docker Compose 部署拓扑

```
docker compose --profile dependencies  →  基础设施（PostgreSQL, Kafka, Redis, OpenSearch）
docker compose --profile hadoop        →  Hadoop 环境（YARN, HDFS, Spark HistoryServer）
docker compose --profile dolphinscheduler → DolphinScheduler 调度系统
docker compose --profile compass-demo  →  Compass 全部服务（单容器）
docker compose --profile compass       →  Compass 生产部署
```

### 6.5 打包与发布

```bash
# 构建分发包
mvn clean package -DskipTests -Pdist

# 输出：dist/compass-v1.1.2.tar.gz
# 包含：bin/start_all.sh, bin/stop_all.sh, conf/, lib/

# 可选前端打包
mvn clean package -DskipTests -Pdist -Pspark    # 含 Spark Web UI
mvn clean package -DskipTests -Pdist -Pflink    # 含 Flink Web UI
```

---

## 七、数据库设计

### 核心表结构

| 表名 | 用途 | 关键字段 |
|------|------|---------|
| user_info | 用户信息 | username, password, is_admin, scheduler_type |
| project | 项目元数据 | project_name, user_id |
| flow | 工作流定义 | flow_name, project_name |
| task | 任务定义 | task_name, flow_name, project_name |
| task_instance | 任务执行实例 | task_name, execution_date, retry_times |
| task_application | 引擎应用映射 | application_id, task_name |
| task_datum | 任务性能基线 | baseline 对比数据 |
| task_diagnosis_advice | 诊断规则配置 | 规则名称、建议内容 |
| blocklist | 诊断黑名单 | 排除特定任务的诊断 |
| flink_task_app | Flink 任务应用 | Flink 相关指标 |
| flink_task_diagnosis | Flink 诊断结果 | 诊断规则、建议 |

---

## 八、存在的问题及优化建议

### 8.1 架构层面

| # | 问题 | 影响 | 优化建议 |
|---|------|------|---------|
| 1 | **模块拆分过细但耦合严重** — task-common 承载了几乎所有领域模型（eventlog、flink、mr、gc、job 等），所有模块都重度依赖它 | 任何领域模型变更都会触发全量编译；模块边界模糊 | 按领域拆分 common：`compass-model-spark`、`compass-model-flink`、`compass-model-scheduler`，各服务仅依赖需要的模型包 |
| 2 | **task-portal 职责过大** — 同时承载 REST API、Spark 诊断报告、Flink 诊断、前端资源托管、定时初始化任务 | 单体化趋势，难以独立扩缩容 | 将 Flink 诊断 API 迁出至 task-flink 服务，Portal 只做 API 网关和前端托管 |
| 3 | **Spring Boot 3.2 + Java 17 与部分旧依赖不兼容** — Swagger 2.x（springfox 3.0.0）、jjwt 0.9.0 等已不兼容 Jakarta EE | 运行时可能报错，安全漏洞 | 迁移至 SpringDoc OpenAPI 3.x，升级 jjwt 至 0.12.x |
| 4 | **Canal + ZooKeeper 仍作为可选依赖保留** — 即使 Kafka 已迁移至 KRaft 模式 | 增加运维复杂度，ZooKeeper 仅为 Canal HA 保留 | 评估是否可以用 Kafka Connect JDBC Source Connector 替代 Canal，彻底去除 ZooKeeper |

### 8.2 代码质量

| # | 问题 | 影响 | 优化建议 |
|---|------|------|---------|
| 5 | **测试覆盖率极低** — 494 个 Java 源文件仅 22 个测试文件，大量核心检测逻辑无测试 | 重构和功能变更风险高 | 优先为 6 个调度层 Detector 和引擎层解析器编写单元测试，目标覆盖率 > 60% |
| 6 | **双 JWT 库共存** — 同时依赖 java-jwt 4.2.2 和 jjwt 0.9.0 | 维护混乱，存在版本冲突风险 | 统一使用一个 JWT 库（推荐 java-jwt 或升级 jjwt） |
| 7 | **大量硬编码配置** — Kafka Topic 名称、OpenSearch 索引名、线程池参数散落在代码各处 | 环境切换困难 | 集中到 application.yml，通过 Spring 配置绑定管理 |
| 8 | **Portal domain 类过多且扁平化** — task-portal 下有 80+ 个 domain 类，部分职责重叠 | 代码导航困难，新人上手慢 | 使用 DTO/VO/Entity 分层，按业务功能分子包 |

### 8.3 数据流与性能

| # | 问题 | 影响 | 优化建议 |
|---|------|------|---------|
| 9 | **OpenSearch 索引策略不明确** — 诊断结果写入 OpenSearch 但无明确的索引生命周期管理 | 长期运行后存储膨胀 | 配置 ISM (Index State Management) 策略，按日/月自动滚动和清理索引 |
| 10 | **task-gpt 无限速/降级机制** — 直接调用 ChatGPT API，无速率限制和故障降级 | API 限流/不可用时级联失败 | 增加 Rate Limiter（如 Resilience4j）、降级为本地规则匹配、配置 API 调用超时 |
| 11 | **AnalyzerBridge Semaphore 可能成为瓶颈** — 检测和解析共享同一 JVM，Semaphore 限制了并行度 | 高吞吐场景下检测线程被阻塞等待解析完成 | 考虑引入背压机制或异步回调模式，避免检测线程阻塞在 `semaphore.acquire()` |

### 8.4 部署与运维

| # | 问题 | 影响 | 优化建议 |
|---|------|------|---------|
| 12 | **缺少健康检查和服务发现** — 各微服务间通过 Kafka 解耦但无统一的健康检查机制 | 服务异常难以快速定位 | 统一启用 Spring Boot Actuator `/health` 端点，接入 Prometheus + Grafana 监控 |
| 13 | **Docker Compose 单机部署，无 K8s 支持** — 生产部署仅提供 shell 脚本和 Docker Compose | 不适合大规模生产环境 | 提供 Helm Chart 或 K8s Manifest，支持水平扩展和滚动更新 |
| 14 | **配置管理分散** — compass_env.sh、application.yml、Docker Compose 环境变量三套配置 | 配置不一致风险高 | 统一使用 Spring Profiles + 环境变量注入，减少配置来源 |
| 15 | **无 CI/CD 流水线** — .github/ 目录存在但无完整的 GitHub Actions 工作流 | 代码质量无自动化保障 | 添加 CI 流水线：编译 → 单元测试 → 代码检查 → Docker 镜像构建 → 推送 |

### 8.5 安全

| # | 问题 | 影响 | 优化建议 |
|---|------|------|---------|
| 16 | **密码明文存储风险** — user_info 表 password 字段未标注加密方式 | 数据泄露风险 | 确认使用 BCrypt 加密，在 SQL schema 注释中明确 |
| 17 | **Docker Compose 硬编码凭证** — compose.yml 中 PostgreSQL、DolphinScheduler 密码明文 | 安全隐患 | 使用 Docker Secrets 或 `.env` 文件（加入 .gitignore） |
| 18 | **Swagger UI 生产环境暴露** — SwaggerConfig 无环境限制 | API 接口暴露 | 仅在 dev/test Profile 下启用 Swagger |

---

## 九、优化路线图（建议）

### 短期（1-2 个月）
1. 补充核心检测器单元测试（覆盖率 > 60%）
2. 统一 JWT 库，修复 Spring Boot 3.x 兼容性问题
3. 添加 GitHub Actions CI 流水线
4. 配置 OpenSearch 索引生命周期管理
5. task-gpt 增加速率限制和降级机制

### 中期（3-6 个月）
6. 拆分 task-common 为领域模型子包
7. 提供 Kubernetes Helm Chart
8. 统一配置管理（Spring Profiles + 环境变量）
9. 接入 Prometheus + Grafana 监控告警
10. 前端增加 Flink 诊断详情页

### 长期（6-12 个月）
11. 评估以 Kafka Connect 替代 Canal
12. 支持更多计算引擎（Presto/Trino、Hive on Tez）
13. 引入机器学习模型替代部分启发式规则
14. 支持多租户和 RBAC 细粒度权限
