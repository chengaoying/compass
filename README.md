# Compass

[中文文档](README_zh.md)

## 简介

Compass 是一个面向大数据生态的计算引擎与调度系统诊断平台，旨在提升问题排查效率、降低调优复杂度。平台自动采集日志与指标数据，利用启发式规则识别问题并提供调优建议。同时集成 ChatGPT，基于 Drain 算法自动聚合日志模板，生成智能诊断建议。

## 核心特性

- **非侵入式诊断** — 无需修改原有平台代码，实时自动诊断
- **多调度系统支持** — 兼容 DolphinScheduler 2.x/3.x、Airflow 2.x
- **Spark 引擎诊断** — 数据倾斜、OOM、CPU/内存浪费、大表扫描、Shuffle 失败等 14+ 种异常检测
- **Flink 实时诊断** — 资源利用率、反压检测、算子性能、数据分布分析
- **调度层诊断** — 任务失败、耗时异常、基线偏差、长期失败任务等
- **ChatGPT 智能分析** — Drain 算法聚合日志模板 + ChatGPT 生成调优建议，LRU 缓存降低成本

## 支持组件

| 组件 | 状态 |
|------|------|
| Spark 2.4+ | ✅ 已支持 |
| Flink 1.2+ | ✅ 已支持 |
| DolphinScheduler 2.x/3.x | ✅ 已支持 |
| Airflow 2.x | ✅ 已支持 |
| ChatGPT 日志分析 | ✅ 已支持 |
| Trino | 🔜 规划中 |
| Hive on Tez | 🔜 规划中 |

## 架构概览

```
┌─────────────────────────────────────────────────────────┐
│                    接入层                                 │
│   YARN ResourceManager  ·  Spark HistoryServer  · Flink  │
│   DolphinScheduler  ·  Airflow                           │
├─────────────────────────────────────────────────────────┤
│                    服务层                                 │
│  task-collector   元数据采集                               │
│  task-syncer      调度系统数据同步                          │
│  task-analyzer    异常检测 + 日志解析                       │
│  task-portal      REST API + Web UI                      │
│  task-gpt         ChatGPT 智能分析                        │
│  task-flink       Flink 实时诊断（可选）                    │
├─────────────────────────────────────────────────────────┤
│                    基础设施层                               │
│  PostgreSQL  ·  Kafka (KRaft)  ·  OpenSearch  ·  Redis   │
└─────────────────────────────────────────────────────────┘
```

## 诊断能力

### 调度层诊断

| 诊断类别 | 说明 |
|---------|------|
| 任务失败 | 运行周期内重试后仍失败的任务 |
| 首次失败 | 首次运行失败但重试后成功的任务 |
| 长期失败 | 每个运行周期都持续失败的任务 |
| 基线偏差 | 运行结束时间早于或晚于正常值 |
| 耗时异常 | 任务耗时相比正常值过短或过长 |
| 长耗时任务 | 运行时间超过 2 小时的任务 |

### Spark 引擎诊断

| 诊断类别 | 维度 | 说明 |
|---------|------|------|
| SQL 失败 | 运行分析 | SQL 语法/字段/权限等错误 |
| Shuffle 失败 | 运行分析 | Shuffle 过程异常导致任务失败 |
| 内存溢出 | 运行分析 | 运行内存不足导致 OOM |
| CPU 浪费 | 资源分析 | CPU 利用率过低 |
| 内存浪费 | 资源分析 | 内存利用率过低 |
| 大表扫描 | 效率分析 | 未分区或无过滤条件导致全表扫描 |
| OOM 预警 | 效率分析 | Broadcast 数据量过大可能引发 OOM |
| 数据倾斜 | 效率分析 | Task 处理数据量最大值远超中位数 |
| Job 耗时异常 | 效率分析 | Job 运行中空闲时间占比过高 |
| Stage 耗时异常 | 效率分析 | Stage 运行中空闲时间占比过高 |
| Task 长尾 | 效率分析 | Task 最大运行时间远超中位数 |
| HDFS 读写卡顿 | 效率分析 | Task 数据处理速率远低于正常 Stage |
| 推测执行过多 | 效率分析 | Executor 处理过慢导致大量推测执行 |
| 全局排序异常 | 效率分析 | 整个 Application 仅包含一个 Task |

### Flink 实时诊断

| 诊断类别 | 维度 | 说明 |
|---------|------|------|
| 内存使用过高/过低 | 资源分析 | TaskManager 内存利用率异常 |
| JobManager 内存异常 | 资源分析 | TaskManager 过多导致 JM 内存异常 |
| 无数据处理 | 资源分析 | 作业无数据流入 |
| 部分 Task 无数据 | 资源分析 | 部分 TaskManager 无数据处理 |
| TaskManager 内存优化 | 资源分析 | TM 内存配置不合理建议优化 |
| 并行度不足 | 资源分析 | Flink 作业并行度过低 |
| CPU 使用过高/过低 | 资源分析 | CPU 利用率异常 |
| 算子慢 | 运行分析 | 存在性能瓶颈算子 |
| 反压 | 运行分析 | 存在数据反压 |
| 高延迟 | 运行分析 | 数据处理延迟过高 |

## 快速开始

### 环境要求

- JDK 17+
- Maven 3.6.0+
- Node.js 14.18+（前端构建）
- pnpm 7.8.0

### 基础设施依赖

- PostgreSQL 10.0+（或 MySQL 5.7+）
- Apache Kafka 3.4+（KRaft 模式，无需 ZooKeeper）
- Redis 7.x
- OpenSearch 2.x

### Docker Compose 部署

```bash
cd docker/playground

# 1. 启动基础设施
docker compose --profile dependencies up -d

# 2. 启动 Compass 服务
docker compose --profile compass-demo up -d
```

### 源码构建

```bash
# 构建分发包
mvn clean package -DskipTests -Pdist

# 输出: dist/compass-v1.1.2.tar.gz

# 启动所有服务
cd dist && tar -xzf compass-*.tar.gz
./bin/start_all.sh
```

### 服务端口

| 服务 | 端口 | 说明 |
|------|------|------|
| task-analyzer | 7071 | 异常检测 + 日志解析 |
| task-collector | 7072 | 元数据采集 |
| task-syncer | 7073 | 数据同步 |
| task-gpt | 7074 | GPT 智能分析 |
| task-portal | 7075 | REST API（`/compass`） |

## 工程结构

```
compass/
├── task-common/          # 公共库：领域模型、工具类
├── task-mbg/             # MyBatis Generator：数据库模型
├── task-analyzer/        # 异常检测 + 日志解析
├── task-collector/       # YARN/Spark 元数据采集
├── task-syncer/          # 调度系统数据同步
├── task-portal/          # REST API + Web 后端
├── task-gpt/             # ChatGPT 日志分析
├── task-flink-core/      # Flink 诊断核心模型
├── task-flink/           # Flink 实时诊断（可选）
├── task-canal/           # Canal CDC 集成（可选）
├── task-canal-adapter/   # Canal 适配器
├── task-ui/              # 前端（Vue 3 + Vite）
├── task-assembly/        # 打包 & 启动脚本
└── docker/               # Docker 部署配置
```

## 技术栈

| 层次 | 技术 | 版本 |
|------|------|------|
| 后端框架 | Spring Boot | 3.2.4 |
| ORM | MyBatis + MyBatis-Plus | 3.5.11 / 3.5.6 |
| 数据库 | PostgreSQL / MySQL | 10.0+ / 5.7+ |
| 搜索引擎 | OpenSearch | 2.13.0 |
| 消息队列 | Kafka (KRaft) | 3.4.0 |
| 缓存 | Redis | 7.x |
| 前端 | Vue 3 + Vite + Element Plus | - |
| AI | ChatGPT + Drain 算法 | - |

## 文档

- [架构文档](document/architecture/PROJECT_ARCHITECTURE.md)
- [部署文档](document/manual/deployment.md)

## 界面预览

![诊断报告](document/manual/img/spark_report.png)
![趋势分析](document/manual/img/spark_report_trend.png)
![任务列表](document/manual/img/spark_scheduler.png)
![内存分析](document/manual/img/application_report_memory.png)

## 社区

欢迎参与 Compass 的使用和开发：

- 提交 [Issue](https://github.com/cubefs/compass/issues)
- 提交 Pull Request，请阅读 [贡献指南](https://github.com/cubefs/compass/blob/main/CONTRIBUTING.md)
- 参与 [讨论](https://github.com/cubefs/compass/discussions)

## 许可证

Compass 基于 [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0) 开源。详情见 [LICENSE](LICENSE) 和 [NOTICE](NOTICE)。

## 参考

Drain 算法基于 `logpai` 项目：
- [https://github.com/logpai/Drain3](https://github.com/logpai/Drain3)
- [https://jiemingzhu.github.io/pub/pjhe_icws2017.pdf](https://jiemingzhu.github.io/pub/pjhe_icws2017.pdf)
