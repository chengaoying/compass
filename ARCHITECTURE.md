# Compass 项目架构分析与优化建议

## 一、项目概述

Compass 是 OPPO 开源的**大数据任务诊断平台**，用于对 Spark、MapReduce、Flink 等大数据引擎任务进行自动化诊断分析，发现性能问题（数据倾斜、内存溢出、CPU浪费、大表扫描等），并给出优化建议。

**技术栈**: Spring Boot 2.7.8 + MyBatis + Kafka + Redis + OpenSearch + MySQL + Vue 3

---

## 二、整体架构

### 2.1 模块总览（14个模块）

```
compass/
├── task-common          # 公共模块：领域模型、常量、工具类、OpenSearch映射
├── task-mbg             # MyBatis Generator：数据库表映射、Mapper接口生成
├── task-portal          # Web API 层：REST接口、前端服务、Swagger文档
├── task-syncer          # 数据同步：调度系统 → Compass（通过Canal/Kafka CDC）
├── task-canal           # Canal Server：监听调度系统MySQL binlog
├── task-canal-adapter   # Canal Adapter：binlog数据适配转发
├── task-application     # 应用解析：从任务实例解析出YARN Application ID
├── task-detect          # 异常检测：任务级别异常检测（耗时、持续失败等）
├── task-parser          # 日志解析：解析Spark/MR Event Log和Executor日志，运行检测器
├── task-metadata        # 元数据采集：定时从YARN/Spark获取集群元数据
├── task-flink-core      # Flink诊断核心：Flink任务诊断逻辑
├── task-flink           # Flink诊断服务：Flink任务诊断服务入口
├── task-assembly        # 打包部署：部署脚本、环境配置
├── task-ui              # 前端：Vue 3 + Vite + Element Plus
```

### 2.2 模块依赖关系

```
task-common ←── task-mbg ←── task-portal
                          ←── task-syncer
                          ←── task-application
                          ←── task-detect
                          ←── task-parser
                          ←── task-metadata
                          ←── task-flink-core ←── task-flink
                                              ←── task-portal (Flink诊断功能)
```

### 2.3 Spring Boot 服务一览

| 服务 | 端口 | 角色 |
|------|------|------|
| task-portal | 7075 | Web API 网关 |
| task-syncer | 7072 | 数据同步服务 |
| task-application | 7074 | Application ID 解析 |
| task-detect | 7071 | 任务异常检测 |
| task-parser | 7073 | 日志深度分析 |
| task-metadata | 7070 | YARN/Spark 元数据采集 |
| task-flink | 7076 | Flink 任务诊断 |
| task-canal | - | Canal binlog监听 |
| task-canal-adapter | - | Canal binlog适配 |

---

## 三、数据流与业务流程

### 3.1 端到端数据流

```
┌─────────────────┐     binlog/CDC      ┌──────────────┐    Kafka:mysqldata     ┌──────────────┐
│ DolphinScheduler│ ──────────────────> │  task-canal   │ ───────────────────> │  task-syncer  │
│   / Airflow     │                     │  (Canal Server)│                      │              │
│  (调度系统MySQL) │                     └──────────────┘                      │ 表映射+写入    │
└─────────────────┘                                                           │ Compass MySQL │
                                                                              └──────┬───────┘
                                                                                     │ Kafka:task-instance
                                                                                     ▼
                                                              ┌──────────────────────────────────────┐
                                                              │            并行处理                     │
                                                              │                                      │
                                                              ▼                      ▼                │
                                                      ┌──────────────┐      ┌──────────────┐         │
                                                      │ task-detect  │      │task-application│        │
                                                      │ (异常检测)    │      │(AppID解析)     │        │
                                                      │              │      │              │         │
                                                      │ →任务耗时异常  │      │ →解析调度日志   │        │
                                                      │ →持续失败检测  │      │ →提取AppID     │        │
                                                      │ →基线对比      │      │ →写入MySQL     │        │
                                                      └──────┬───────┘      └──────┬───────┘         │
                                                             │                     │                  │
                                                             ▼                     ▼                  │
                                                      ┌─────────────┐      ┌──────────────┐          │
                                                      │ OpenSearch   │      │   Redis      │          │
                                                      │ (job-analysis│      │ (日志解析队列)  │         │
                                                      │  job-instance│      └──────┬───────┘          │
                                                      │  detector)   │             │                  │
                                                      └─────────────┘             ▼                  │
                                                                          ┌──────────────┐           │
                                                                          │ task-parser   │           │
                                                                          │ (日志深度分析)  │          │
                                                                          │              │           │
                                                                          │ →读取HDFS日志  │          │
                                                                          │ →Spark EventLog│         │
                                                                          │ →MR History    │          │
                                                                          │ →GC Log分析    │          │
                                                                          │ →运行检测器     │          │
                                                                          └──────┬───────┘           │
                                                                                 │                   │
                                                                                 ▼                   │
                                                                          ┌─────────────┐           │
                                                                          │ OpenSearch   │           │
                                                                          │ (task-app    │           │
                                                                          │  log-summary │           │
                                                                          │  gc-log      │           │
                                                                          │  spark-app)  │           │
                                                                          └─────────────┘           │
                                                                                                    │
┌──────────────┐      ┌──────────────┐                                                              │
│ task-metadata│      │  task-flink  │ ← Kafka:task-application / flink-task-app                     │
│ (元数据采集)  │      │ (Flink诊断)  │                                                              │
│ →YARN API    │      │ →Prometheus  │                                                              │
│ →Spark API   │      │ →资源优化     │                                                              │
└──────┬───────┘      └──────┬───────┘                                                              │
       │                     │                                                                      │
       ▼                     ▼                                                                      │
┌─────────────┐      ┌─────────────┐                                                                │
│ OpenSearch   │      │ OpenSearch   │                                                               │
│ (yarn-app    │      │(flink-report │                                                              │
│  spark-app)  │      │ flink-task   │                                                              │
└─────────────┘      │ -analysis)   │                                                               │
                     └─────────────┘                                                                │
                                                                                                    │
                     ┌──────────────┐    ←── 查询所有OpenSearch索引 + MySQL                            │
                     │ task-portal  │                                                                │
                     │ (Web API)    │                                                                │
                     │ Port: 7075   │                                                                │
                     └──────────────┘                                                                │
                           ↑                                                                        │
                     ┌──────────────┐                                                                │
                     │  task-ui     │                                                                │
                     │ (Vue 3前端)   │                                                               │
                     └──────────────┘                                                                │
```

### 3.2 Kafka Topic 流转

| Topic | 生产者 | 消费者 | 内容 |
|-------|--------|--------|------|
| `mysqldata` | task-canal | task-syncer | 调度系统MySQL binlog变更 |
| `task-instance` | task-syncer | task-application, task-detect | 任务实例状态变更（已映射到Compass格式）|
| `task-application` | task-application | task-flink | YARN Application信息 |
| `flink-task-app` | (外部/task-flink) | task-flink | Flink任务App信息 |

### 3.3 OpenSearch 索引

| 索引名 | 写入者 | 用途 |
|--------|--------|------|
| `compass-log-summary` | task-parser | 日志解析摘要 |
| `compass-task-app` | task-parser | Spark/MR应用诊断结果 |
| `compass-job-analysis` | task-detect | 任务级异常诊断 |
| `compass-detector-app` | task-parser | 检测器详细结果 |
| `compass-gc-log` | task-parser | GC日志分析 |
| `compass-yarn-app` | task-metadata | YARN应用元数据 |
| `compass-spark-app` | task-metadata | Spark应用元数据 |
| `compass-job-instance` | task-detect | 任务实例快照 |
| `compass-flink-report` | task-flink | Flink诊断报告 |
| `compass-flink-task-analysis` | task-flink | Flink任务分析 |

### 3.4 MySQL 核心表

| 表名 | 用途 |
|------|------|
| `user` | 用户（从调度系统同步） |
| `project` | 项目（从调度系统同步） |
| `flow` | 工作流/DAG（从调度系统同步） |
| `task` | 任务定义（从调度系统同步） |
| `task_instance` | 任务运行实例（从调度系统同步+状态映射） |
| `task_application` | 任务与YARN Application关联 |
| `blocklist` | 诊断黑名单 |
| `task_diagnosis_advice` | 诊断建议模板（41条预置规则） |
| `task_datum` | 任务基线数据 |
| `flink_task` | Flink任务元数据 |
| `flink_task_app` | Flink Application详情 |

---

## 四、功能架构

### 4.1 Spark/MapReduce 诊断

**检测器列表**（task-parser模块）：

| 检测器 | 检测内容 |
|--------|----------|
| DataSkewDetector | 数据倾斜检测 |
| LargeTableScanDetector | 大表扫描检测 |
| CpuWasteDetector | CPU资源浪费 |
| MemWasteDetector | 内存资源浪费 |
| OOMWarnDetector | OOM风险预警 |
| JobDurationDetector | Job耗时异常 |
| StageDurationDetector | Stage耗时异常 |
| TaskDurationDetector | Task耗时异常 |
| SpeculativeTaskDetector | 推测执行过多 |
| GlobalSortDetector | 全局排序异常 |
| HdfsStuckDetector | HDFS卡顿检测 |
| MRDataSkewDetector | MR数据倾斜 |
| MRGCDetector | MR GC分析 |
| MRMemoryWasteDetector | MR内存浪费 |
| MRTaskDurationDetector | MR Task耗时异常 |
| MRSpeculativeTaskDetector | MR推测执行 |
| MRLargeTableScanDetector | MR大表扫描 |

**日志解析器**：
- `SparkEventLogParser` - 解析Spark Event Log（Job/Stage/Task维度）
- `SparkExecutorLogParser` - 解析Spark Executor日志（错误、异常）
- `MapReduceJobHistoryParser` - 解析MR Job History
- `MapReduceContainerLogParser` - 解析MR Container日志
- `SchedulerLogParser` - 解析调度器日志（提取Application ID）
- `CommonTextParser` - 通用文本日志解析

### 4.2 Flink 诊断

Flink诊断通过Prometheus采集运行时指标，分析资源使用情况：
- 并行度优化建议
- TM 内存/CPU 优化
- JM 内存优化
- Slot 配置优化
- 运行时异常检测

### 4.3 任务级异常检测（task-detect）

- **耗时异常**：任务运行时间超过历史基线
- **持续失败**：连续N天（默认10天）失败
- **首次失败**：之前成功的任务突然失败
- **基线对比**：与历史正常运行数据对比

### 4.4 Web API（task-portal）

| API 路径 | 功能 |
|----------|------|
| `/api/v1/job/*` | 任务诊断列表、详情、图表、基线 |
| `/api/v1/app/*` | Application诊断报告（运行错误/资源/耗时）|
| `/api/v1/app/diagnose` | 一键诊断 |
| `/api/v1/app/gc` | GC日志分析 |
| `/api/v1/report/*` | 统计报告、项目列表 |
| `/api/flink/*` | Flink任务诊断、资源优化建议 |
| `/api/v1/blocklist/*` | 诊断黑名单管理 |
| `/api/v1/user/*` | 用户管理 |

---

## 五、需要优化的问题与建议

### 5.1 架构层面

#### P0 - 严重问题

**1. 微服务拆分过细，运维复杂度高**
- 9个独立Spring Boot服务 + Canal，对于一个诊断工具来说拆分过细
- 每个服务都独立配置MySQL/Redis/OpenSearch连接，配置重复且容易不一致
- **建议**：考虑将 task-application + task-detect + task-parser 合并为一个"诊断引擎"服务；将 task-canal + task-canal-adapter + task-syncer 合并为一个"数据同步"服务。最终缩减为 4 个服务：数据同步、诊断引擎、元数据采集、Web API

**2. 配置管理混乱**
- 每个模块独立维护 `application.yml`，存在大量重复配置
- 环境变量与配置文件混用，`compass_env.sh` 中的变量未在所有模块生效
- 没有使用配置中心（如Nacos/Apollo）
- **建议**：引入 Spring Cloud Config 或 Nacos 配置中心；或至少将公共配置抽取到 `application-common.yml`

**3. Spring Boot 版本过旧**
- Spring Boot 2.7.8 已于 2023年11月停止维护
- Java 8 target 限制了现代语言特性的使用
- **建议**：升级到 Spring Boot 3.2.x + Java 17

#### P1 - 重要问题

**4. 数据同步链路过长**
- 调度系统 → Canal → Kafka → task-syncer → MySQL → Kafka → task-application/task-detect
- Canal + Canal-Adapter + Syncer 三个组件完成一件事（表数据同步），链路过长、故障点多
- **建议**：使用 Debezium 替代 Canal（更活跃的社区、更好的容错）；或直接使用 task-syncer 轮询调度系统数据库

**5. Redis 使用不规范**
- 使用 Redis 做延迟队列（{lua}:delayed:task），自行实现延迟逻辑
- 缺少Redis集群配置的高可用设置
- **建议**：使用 Redis Stream 或专门的消息队列中间件处理延迟任务

**6. OpenSearch 索引设计需优化**
- 10 个索引，部分索引 shards=10 replicas=2，对于非大规模部署过重
- 缺少索引生命周期管理（ILM/ISM），历史数据无自动清理
- **建议**：添加基于时间的索引滚动策略；根据实际数据量调整 shard 数；添加 ISM 策略自动清理过期数据

### 5.2 代码质量

#### P1 - 重要问题

**7. 异常处理不规范**
- Controller 层大量 `throws Exception`，缺少统一异常处理
- 如 `FlinkTaskDiagnosisController.diagnosis()` 方法（353行）直接在Controller中编写复杂业务逻辑
- **建议**：添加 `@ControllerAdvice` 全局异常处理器；Controller只做参数接收和转发

**8. Controller 职责不清**
- `FlinkTaskDiagnosisController` 直接注入 `FlinkTaskAppMapper` 和 `BlocklistMapper`，绕过Service层
- Controller 中包含业务逻辑（黑名单校验、数据库查询）
- **建议**：严格遵循 Controller → Service → DAO 的分层；将所有业务逻辑移到Service层

**9. 硬编码中文字符串**
- `FlinkTaskDiagnosisController` 中大量硬编码中文字符串作为配置项
  - `orderMap.put("tm_num * tm_mem - diagnosis_tm_num * diagnosis_tm_mem_size", "任务可优化总内存数")`
- **建议**：使用国际化（i18n）或配置文件管理

**10. 反射滥用**
- `GCReport.genDoc()` 使用反射遍历字段生成Map
- **建议**：使用 Jackson ObjectMapper 或 BeanUtils 替代手写反射

**11. 类型安全缺失**
- Controller 返回类型大量使用 `CommonStatus<?>`，丢失类型信息
- **建议**：使用具体泛型类型 `CommonStatus<JobListResponse>` 等

### 5.3 安全问题

#### P0 - 严重问题

**12. JWT Secret 硬编码**
- `application.yml` 中 `jwt.secret: 8ff3bf2c8344`，明文硬编码且过短
- **建议**：使用环境变量或密钥管理服务注入；Secret 至少 256 位

**13. 数据库凭据明文配置**
- 所有 `application.yml` 中 MySQL/Redis/OpenSearch 密码明文存储
- **建议**：使用 Jasypt 加密或环境变量注入

**14. Swagger 生产环境暴露**
- 无环境区分，Swagger UI 在生产环境也可访问
- **建议**：仅在 dev/test profile 下启用 Swagger

### 5.4 性能与可靠性

#### P1 - 重要问题

**15. Kafka 消费者缺少错误处理**
- `ConsumerMessage` (task-application) 中 catch Exception 后仅 log.error，消息被丢弃
- 手动 `consumer.commitSync()` 可能导致消息丢失
- **建议**：引入死信队列（DLQ）；使用Spring Kafka的错误处理器和重试机制

**16. 线程池配置缺少监控**
- 多个模块使用自定义线程池，但缺少监控指标
- **建议**：暴露线程池指标到 Prometheus/Actuator

**17. 数据库连接池偏小**
- Druid 配置 `initial-size: 5, min-idle: 10, max-active: 20`
- 多个服务共享同一数据库，并发量可能不足
- **建议**：根据实际并发量调整；建议 max-active 至少 50

### 5.5 工程实践

#### P2 - 建议改进

**18. 缺少单元测试**
- 仅发现 `JobControllerTest` 一个测试文件
- 核心检测逻辑（检测器、解析器）几乎无测试覆盖
- **建议**：为每个检测器添加单元测试，至少覆盖核心业务逻辑

**19. 日志规范需统一**
- 部分使用 `log.info`，部分使用 `System.out`
- MyBatis 配置了 `StdOutImpl`（标准输出日志），不适合生产环境
- **建议**：统一使用 SLF4J；移除 StdOutImpl 配置

**20. API 版本管理不一致**
- Spark/MR API: `/api/v1/job/*`、`/api/v1/app/*`
- Flink API: `/api/flink/*`（无版本号）
- **建议**：统一 API 版本策略

**21. 使用过时的Swagger注解**
- 使用 Swagger 2 (`io.swagger.annotations`)
- **建议**：迁移到 SpringDoc OpenAPI 3

**22. `allow-circular-references: true`**
- Spring 配置允许循环依赖，说明存在不合理的Bean依赖
- **建议**：排查并消除循环依赖

---

## 六、推荐优化路线图

### Phase 1 - 安全加固（1-2周）
- [ ] JWT Secret 外部化 + 增强强度
- [ ] 数据库凭据外部化（环境变量/Vault）
- [ ] 生产环境禁用 Swagger
- [ ] 添加全局异常处理器

### Phase 2 - 代码质量提升（2-4周）
- [ ] Controller 瘦身：业务逻辑移至 Service 层
- [ ] 统一异常处理和日志规范
- [ ] 消除循环依赖
- [ ] 添加核心检测器单元测试

### Phase 3 - 架构优化（1-2月）
- [ ] 服务合并：9个服务 → 4个服务
- [ ] 引入配置中心（Nacos）
- [ ] Kafka 消费者容错增强（DLQ + 重试）
- [ ] OpenSearch 索引生命周期管理

### Phase 4 - 技术升级（2-3月）
- [ ] Spring Boot 2.7 → 3.2 + Java 17
- [ ] Swagger 2 → SpringDoc OpenAPI 3
- [ ] Canal → Debezium（可选）
- [ ] 前端升级优化
