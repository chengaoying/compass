# Compass 部署文档

## 一、项目概述

Compass 是一个大数据任务诊断平台，支持对 DolphinScheduler、Airflow 等调度平台的任务进行异常检测、日志解析和资源优化诊断。

---

## 二、外部基础设施依赖

| 依赖组件       | 版本要求         | 是否必须 | 说明                                           |
|---------------|-----------------|---------|-----------------------------------------------|
| **PostgreSQL** | 10.0+           | 是      | 默认元数据存储数据库                              |
| **MySQL**      | 5.7+            | 否      | 可替代 PostgreSQL 作为元数据存储（需手动下载 mysql-connector-java） |
| **Kafka**      | 3.4.0（推荐）    | 是      | 消息队列，用于 CDC 数据同步和任务实例事件传递          |
| **Redis**      | 任意版本          | 是      | 缓存和日志队列，需以集群模式部署                     |
| **Zookeeper**  | 3.4.5+          | 是      | Canal 所需                                     |
| **OpenSearch** | 1.3.12+         | 是      | 诊断结果存储和查询（兼容 Elasticsearch 7.0+）       |
| **Canal**      | 1.1.6+          | 否      | 仅在使用 DolphinScheduler/Airflow 时需要，用于 CDC  |
| **HDFS**       | Hadoop 3.x      | 否      | 任务日志存储                                     |
| **YARN**       | Hadoop 3.x      | 否      | 应用元数据采集                                   |
| **Spark HistoryServer** | -      | 否      | Spark 应用元数据采集                              |
| **Prometheus** | -               | 否      | 仅 Flink 诊断需要                                |

---

## 三、编译环境依赖

| 依赖          | 版本要求    | 说明                |
|--------------|-----------|---------------------|
| **JDK**      | 8         | Java 编译运行环境     |
| **Maven**    | 3.6.0+    | 构建工具              |
| **Node.js**  | -         | 前端 task-ui 编译（可选）|

---

## 四、Maven 依赖清单（Java 组件）

### 4.1 框架依赖

| 依赖                                         | 版本          | 说明                    |
|---------------------------------------------|--------------|------------------------|
| `spring-boot-starter-parent`                | 3.2.14       | Spring Boot 基础框架     |
| `spring-kafka`                              | 2.9.0        | Kafka 集成              |
| `spring-boot-starter-test`                  | (Boot 管理)   | 测试框架                |

### 4.2 数据库 & ORM

| 依赖                                         | 版本          | 说明                    |
|---------------------------------------------|--------------|------------------------|
| `mybatis-plus-boot-starter`                 | 3.5.6        | MyBatis-Plus ORM       |
| `mybatis`                                   | 3.5.11       | MyBatis 核心            |
| `mybatis-generator-core`                    | 1.4.0        | 代码生成器               |
| `pagehelper-spring-boot-starter`            | 1.4.5        | 分页插件                |
| `pagehelper`                                | 5.3.2        | 分页核心                |
| `druid-spring-boot-starter`                 | 1.2.15       | 数据库连接池             |
| `dynamic-datasource-spring-boot-starter`    | 3.1.0        | 多数据源支持             |
| `mysql-connector-j`                         | 8.0.33       | MySQL 驱动              |
| `postgresql`                                | 42.6.0       | PostgreSQL 驱动         |

### 4.3 搜索引擎

| 依赖                                         | 版本          | 说明                    |
|---------------------------------------------|--------------|------------------------|
| `opensearch-rest-high-level-client`         | 2.13.0       | OpenSearch 客户端        |
| `opensearch-x-content`                      | 2.13.0       | OpenSearch 内容序列化    |

### 4.4 消息队列

| 依赖                                         | 版本          | 说明                    |
|---------------------------------------------|--------------|------------------------|
| `kafka-clients`                             | 3.4.0        | Kafka 原生客户端         |

### 4.5 大数据组件

| 依赖                                         | 版本          | 说明                    |
|---------------------------------------------|--------------|------------------------|
| `hadoop-common`                             | 3.3.4        | Hadoop 通用库           |
| `hadoop-client`                             | 3.3.4        | HDFS/YARN 客户端         |
| `lz4-java`                                 | 1.8.0        | LZ4 压缩               |
| `zstd-jni`                                 | 1.5.5-2      | Zstandard 压缩          |

### 4.6 工具库

| 依赖                                         | 版本          | 说明                    |
|---------------------------------------------|--------------|------------------------|
| `guava`                                     | 31.1-jre     | Google 工具库            |
| `commons-lang3`                             | 3.12.0       | Apache 通用工具          |
| `commons-pool2`                             | 2.11.1       | 对象池（Redis 连接池）    |
| `java-jwt` (com.auth0)                     | 4.2.2        | JWT 认证                |

### 4.7 监控 & 文档

| 依赖                                         | 版本          | 说明                    |
|---------------------------------------------|--------------|------------------------|
| `micrometer-registry-prometheus`            | 1.12.4       | Prometheus 指标暴露      |
| `springdoc-openapi-starter-webmvc-ui`       | 2.5.0        | Swagger/OpenAPI 文档     |

### 4.8 日志

| 依赖                                         | 版本          | 说明                    |
|---------------------------------------------|--------------|------------------------|
| `log4j-api`                                 | 2.17.2       | Log4j2 API             |
| `log4j-core`                                | 2.17.2       | Log4j2 核心             |

### 4.9 测试

| 依赖                                         | 版本          | 说明                    |
|---------------------------------------------|--------------|------------------------|
| `junit-jupiter`                             | 5.8.2        | JUnit 5 测试框架         |
| `mockito-inline`                            | 4.3.1        | Mock 测试               |
| `testcontainers`                            | 1.19.0       | 容器化集成测试            |
| `derby`                                     | 10.14.2.0    | 嵌入式数据库（测试）       |

### 4.10 安全漏洞修复版本

| 依赖                       | 版本          | 说明          |
|---------------------------|--------------|--------------|
| `commons-beanutils`       | 1.9.4        | 安全修复      |
| `commons-collections`     | 3.2.2        | 安全修复      |
| `commons-net`             | 3.9.0        | 安全修复      |
| `protobuf-java`           | 3.21.12      | 安全修复      |
| `snakeyaml`               | 1.33         | 安全修复      |
| `woodstox-core`           | 5.4.0        | 安全修复      |
| `jettison`                | 1.5.3        | 安全修复      |
| `avro`                    | 1.11.1       | 安全修复      |

---

## 五、模块说明与服务端口

| 模块               | 端口   | 功能说明                                            |
|-------------------|-------|---------------------------------------------------|
| **task-portal**    | 7075  | REST API 层 + 前端，提供诊断结果展示、一键诊断等功能       |
| **task-syncer**    | 7076  | 调度平台元数据同步（CDC → Compass 标准模型）             |
| **task-collector** | 7070  | 数据采集：关联 applicationId、同步 Yarn/Spark 元数据     |
| **task-analyzer**  | 7071  | 异常检测 + 日志解析诊断（Spark/MR）                    |
| **task-flink**     | 7001  | Flink 任务诊断                                      |
| **task-gpt**       | -     | ChatGPT 日志模板解读（可选）                           |
| **task-canal**     | -     | Canal Server，订阅调度平台 MySQL binlog               |
| **task-canal-adapter** | 8181 | Canal Adapter，调度平台原始表同步到 Compass            |

---

## 六、Kafka Topics

| Topic              | 生产者          | 消费者                              | 用途                    |
|-------------------|----------------|-------------------------------------|------------------------|
| `mysqldata`       | Canal Server    | task-syncer, task-canal-adapter     | CDC binlog 事件          |
| `task-instance`   | task-syncer     | task-collector, task-analyzer       | 任务实例执行记录           |
| `task-application`| task-collector  | task-flink                          | Yarn 应用元数据关联        |
| `flink-task-app`  | task-flink      | task-flink                          | Flink 特定应用元数据       |
| `exception-log`   | task-analyzer   | (可选)                              | 异常日志                  |

---

## 七、OpenSearch 索引

| 索引名                        | 写入模块        | 读取模块       | 用途                  |
|------------------------------|----------------|---------------|----------------------|
| `compass-job-analysis`       | task-analyzer  | task-portal   | 工作流级异常分析        |
| `compass-job-instance`       | task-analyzer  | task-portal   | 工作流实例执行记录       |
| `compass-task-app`           | task-collector | task-portal   | 任务-应用关联           |
| `compass-log-summary`        | task-analyzer  | task-portal   | 日志解析摘要           |
| `compass-yarn-app`           | task-collector | task-portal   | Yarn 应用指标          |
| `compass-spark-app`          | task-collector | task-portal   | Spark 应用指标         |
| `compass-flink-report`       | task-flink     | task-portal   | Flink 任务报告         |
| `compass-flink-task-analysis`| task-flink     | task-portal   | Flink 诊断优化结果      |
| `compass-detector-app`       | task-analyzer  | task-portal   | 异常检测结果           |

---

## 八、数据库表结构

Compass MySQL/PostgreSQL 数据库包含以下核心表：

| 表名                      | 说明                         |
|--------------------------|------------------------------|
| `user_info`              | 用户表（登录/权限）             |
| `project`                | 项目定义                      |
| `flow`                   | 工作流/DAG 定义                |
| `task`                   | 任务定义                      |
| `task_instance`          | 任务执行实例                   |
| `task_application`       | 任务-Yarn Application 映射     |
| `blocklist`              | 诊断屏蔽名单                   |
| `task_syncer_init`       | task-syncer 初始化标记          |
| `task_diagnosis_advice`  | 诊断建议模板                   |
| `task_datum`             | 任务基线数据                   |
| `template`               | 日志聚类模板                   |
| `flink_task`             | Flink 任务元数据               |
| `flink_task_app`         | Flink Application 详情         |

SQL 初始化文件：
- MySQL: `document/sql/compass_mysql.sql`
- PostgreSQL: `document/sql/compass_postgresql.sql`
- DolphinScheduler 参考表: `document/sql/dolphinscheduler3.sql`

---

## 九、编译与部署步骤

### 9.1 编译打包

```bash
git clone https://github.com/cubefs/compass.git
cd compass
# 全量打包
mvn clean package -DskipTests -Pdist
# 仅打包 Spark Web UI
mvn clean package -DskipTests -Pdist,spark
```

### 9.2 Docker Compose 部署（推荐）

```bash
cp dist/compass-v1.1.2.tar.gz docker/playground
cd docker/playground/
# 启动基础设施（Kafka、MySQL、Redis、OpenSearch、Zookeeper）
docker compose --profile dependencies up -d
# 启动 Compass 服务
docker compose --profile compass-demo up -d
```

### 9.3 手动部署

#### 步骤 1：初始化数据库

```bash
# MySQL
mysql -u root -p < document/sql/compass_mysql.sql
# PostgreSQL
psql -U postgres -f document/sql/compass_postgresql.sql
```

#### 步骤 2：创建 Kafka Topics

```bash
kafka-topics.sh --create --topic mysqldata --bootstrap-server localhost:9092
kafka-topics.sh --create --topic task-instance --bootstrap-server localhost:9092
kafka-topics.sh --create --topic task-application --bootstrap-server localhost:9092
kafka-topics.sh --create --topic exception-log --bootstrap-server localhost:9092
```

#### 步骤 3：配置环境变量

编辑 `bin/compass_env.sh`，设置以下关键参数：

```bash
# 调度平台类型
export SCHEDULER="dolphinscheduler"   # 或 airflow / custom
export SPRING_PROFILES_ACTIVE="hadoop,${SCHEDULER}"

# 调度平台数据库（Canal 订阅源）
export SCHEDULER_MYSQL_ADDRESS="localhost:3306"
export SCHEDULER_MYSQL_DB="dolphinscheduler"
export SCHEDULER_DATASOURCE_URL="jdbc:mysql://${SCHEDULER_MYSQL_ADDRESS}/${SCHEDULER_MYSQL_DB}?..."
export SCHEDULER_DATASOURCE_USERNAME="root"
export SCHEDULER_DATASOURCE_PASSWORD="password"

# Compass 数据库
export DATASOURCE_TYPE="mysql"   # 或 postgresql
export COMPASS_DATASOURCE_ADDRESS="localhost:3306"
export COMPASS_DATASOURCE_DB="compass"
export SPRING_DATASOURCE_URL="jdbc:${DATASOURCE_TYPE}://${COMPASS_DATASOURCE_ADDRESS}/${COMPASS_DATASOURCE_DB}"
export SPRING_DATASOURCE_USERNAME="root"
export SPRING_DATASOURCE_PASSWORD="password"

# Kafka
export SPRING_KAFKA_BOOTSTRAPSERVERS="localhost:9092"

# Redis（集群模式）
export SPRING_REDIS_CLUSTER_NODES="localhost:6379"

# Zookeeper
export SPRING_ZOOKEEPER_NODES="localhost:2181"

# OpenSearch
export SPRING_OPENSEARCH_NODES="localhost:9200"
```

#### 步骤 4：配置 Hadoop（可选）

编辑 `conf/application-hadoop.yml`：

```yaml
hadoop:
  namenodes:
    - nameservices: logs-hdfs
      namenodesAddr: ["nn1.example.com", "nn2.example.com"]
      namenodes: ["nn1", "nn2"]
      user: hdfs
      port: 8020
      matchPathKeys: ["flume"]
  yarn:
    - clusterName: "bigdata"
      resourceManager: ["rm1:8088"]
      jobHistoryServer: "jhs:19888"
  spark:
    sparkHistoryServer: ["shs:18080"]
```

#### 步骤 5：启动所有服务

```bash
./bin/start_all.sh
```

停止所有服务：
```bash
./bin/stop_all.sh
```

---

## 十、历史数据同步

### MySQL → MySQL（使用 Canal Adapter）

```bash
# DolphinScheduler 表
curl "localhost:8181/etl/rdb/mysql1/t_ds_user.yml" -X POST
curl "localhost:8181/etl/rdb/mysql1/t_ds_project.yml" -X POST
curl "localhost:8181/etl/rdb/mysql1/t_ds_process_definition.yml" -X POST
curl "localhost:8181/etl/rdb/mysql1/t_ds_task_definition.yml" -X POST
curl "localhost:8181/etl/rdb/mysql1/t_ds_task_instance.yml" -X POST
curl "localhost:8181/etl/rdb/mysql1/t_ds_process_instance.yml" -X POST
curl "localhost:8181/etl/rdb/mysql1/t_ds_process_task_relation.yml" -X POST

# Airflow 表
curl "localhost:8181/etl/rdb/mysql1/airflow_db_ab_user.yml" -X POST
curl "localhost:8181/etl/rdb/mysql1/airflow_db_dag.yml" -X POST
curl "localhost:8181/etl/rdb/mysql1/airflow_db_dag_run.yml" -X POST
curl "localhost:8181/etl/rdb/mysql1/airflow_db_task_instance.yml" -X POST
```

### MySQL → PostgreSQL（使用 pgloader）

```sql
LOAD DATABASE
     FROM mysql://root:password@localhost:3306/dolphinscheduler
     INTO postgresql://postgres@localhost:5432/compass
     ALTER SCHEMA 'dolphinscheduler' RENAME TO 'public'
     INCLUDING ONLY TABLE NAMES MATCHING
       't_ds_process_definition','t_ds_process_instance',
       't_ds_process_task_relation','t_ds_project',
       't_ds_task_definition','t_ds_task_instance','t_ds_user';
```

---

## 十一、访问地址

| 服务           | URL                                              |
|---------------|--------------------------------------------------|
| Web UI        | `http://localhost:7075/compass/`                 |
| Swagger API   | `http://localhost:7075/compass/swagger-ui/index.html` |

**默认账号**：使用 `custom` 模式时，默认用户名/密码为 `compass / compass`。使用 DolphinScheduler/Airflow 模式时，账号与调度平台一致。

---

## 十二、数据流转全景图

```
调度平台 (DolphinScheduler / Airflow)
         │  MySQL Binlog
         ▼
    [Canal Server] ──→ Kafka[mysqldata]
         │                    │
         ▼                    ▼
  [Canal Adapter]       [task-syncer]
  (原始表同步)          (字段映射转换)
                             │
                    ┌────────┼────────┐
                    ▼        ▼        ▼
              Compass DB   Kafka[task-instance]
                             │
               ┌─────────────┼─────────────┐
               ▼             ▼             ▼
        task-collector  task-analyzer   (其他消费者)
        (采集appId,     (异常检测,
         Yarn/Spark)     日志解析)
               │             │
               ▼             ▼
        Kafka[task-app]  OpenSearch
               │         (job-analysis,
               ▼          log-summary)
          task-flink
          (Flink诊断)
               │
               ▼
          OpenSearch
        (flink-report,
         flink-task-analysis)
               │
               ▼
          task-portal (REST API + WebUI)
```
