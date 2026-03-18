# Compass 模块介绍

## 工程目录

```
compass
├── bin
│   ├── compass_env.sh                  环境变量，基础组件配置
│   ├── start_all.sh                    启动脚本
│   └── stop_all.sh                     停止脚本
├── conf
│   └── application-hadoop.yml          hadoop相关配置
├── task-collector                      采集任务实例、关联applicationId及HDFS日志路径、同步Yarn/Spark元数据
├── task-canal                          订阅调度平台MySQL表元数据到Kafka
├── task-canal-adapter                  同步调度平台MySQL表元数据Compass平台
├── task-analyzer                       工作流层异常检测和引擎层日志解析诊断
├── task-portal                         异常任务的可视化服务
├── task-flink                          Flink任务资源及异常诊断
├── task-flink-core                     Flink任务诊断规则逻辑
├── task-gpt                            聚合日志模板，并使用chatgpt给模板解决方案
└── task-syncer                         调度平台任务关系表的抽象和映射
```

## 历史数据同步

如果调度平台的数据库是MySQL，Compass数据库是PostgreSQL，可使用[pgloader](https://github.com/dimitri/pgloader)创建依赖表和同步历史全量数据

同步dolphinscheduler表：
```
LOAD DATABASE
     FROM mysql://root:password@localhost:3306/dolphinscheduler
     INTO postgresql://postgres@localhost:5432/compass
     ALTER SCHEMA 'dolphinscheduler' RENAME TO 'public'
     INCLUDING ONLY TABLE NAMES MATCHING 't_ds_process_definition','t_ds_process_instance','t_ds_process_task_relation','t_ds_project','t_ds_task_definition','t_ds_task_instance','t_ds_user';
```

同步airflow表：
```
LOAD DATABASE
     FROM mysql://root:password@localhost:3306/airflow
     INTO postgresql://postgres@localhost:5432/compass_airflow
     ALTER SCHEMA 'airflow' RENAME TO 'public'
     INCLUDING ONLY TABLE NAMES MATCHING 'task_instance','dag_run','ab_user','dag','serialized_dag'
     ALTER TABLE NAMES MATCHING 'task_instance' RENAME TO 'tb_task_instance'
     ALTER TABLE NAMES MATCHING 'dag_run'  RENAME TO 'tb_dag_run'
     ALTER TABLE NAMES MATCHING 'ab_user'  RENAME TO 'tb_ab_user'
     ALTER TABLE NAMES MATCHING 'dag'  RENAME TO 'tb_dag'
     ALTER TABLE NAMES MATCHING 'serialized_dag'  RENAME TO 'tb_serialized_dag';
```

如果调度平台的数据库是MySQL，Compass数据库是MySQL，可使用task-canal-adapter接口进行同步历史全量数据

同步dolphinscheduler表：

```shell
curl "localhost:8181/etl/rdb/mysql1/t_ds_process_definition.yml" -X POST

curl "localhost:8181/etl/rdb/mysql1/t_ds_process_instance.yml" -X POST

curl "localhost:8181/etl/rdb/mysql1/t_ds_process_task_relation.yml" -X POST

curl "localhost:8181/etl/rdb/mysql1/t_ds_project.yml" -X POST

curl "localhost:8181/etl/rdb/mysql1/t_ds_task_definition.yml" -X POST

curl "localhost:8181/etl/rdb/mysql1/t_ds_task_instance.yml" -X POST

curl "localhost:8181/etl/rdb/mysql1/t_ds_user.yml" -X POST
```

同步airflow表：

```shell
curl "localhost:8181/etl/rdb/mysql1/airflow_db_ab_user.yml" -X POST

curl "localhost:8181/etl/rdb/mysql1/airflow_db_dag_run.yml" -X POST

curl "localhost:8181/etl/rdb/mysql1/airflow_db_dag.yml" -X POST

curl "localhost:8181/etl/rdb/mysql1/airflow_db_task_instance.yml" -X POST
```

## task-canal

如果您使用的是[DolphinScheduler](https://github.com/apache/dolphinscheduler)
或者[Airflow](https://github.com/apache/airflow)或者自研的等调度平台，
元数据存储在MySQL，可使用[canal.deployer](https://github.com/alibaba/canal/releases/download/canal-1.1.6/canal.deployer-1.1.6.tar.gz)
订阅MySQL binlog同步到Kafka，默认topic是mysqldata。

```
task-canal
├── bin
│   ├── compass_env.sh           compass环境变量
│   ├── init_canal.sh            下载canal.deployer依赖包
│   ├── restart.sh         
│   ├── startup.sh
│   └── stop.sh
├── canal.deployer-1.1.6.tar.gz   compass不提供canal依赖包，可通过init_canal.sh下载，若无网络则自行下载到task-canal根目录
├── conf
│   ├── example
│   │   ├── instance.properties   源MySQL配置和库表配置
│   ├── canal_local.properties    zk,kafka等配置
│   ├── canal.properties
│   ├── logback.xml
├── lib
└── plugin
```

### 核心配置

conf/example/instance.properties

```
canal.instance.master.address=localhost:33066
canal.instance.dbUsername=root
canal.instance.dbPassword=root
canal.instance.filter.regex=.*\\..*
canal.mq.topic=mysqldata

# 动态topic和分区默认不配置，若数据量比较大，可按表Hash到相同topic不同分区，避免单分区压力过大
canal.mq.dynamicTopic = mysqldata:db\\..*
canal.mq.partitionsNum =  12
canal.mq.partitionHash = .*\\..*
```

conf/canal.properties

```
canal.zkServers = localhost:2181
canal.serverMode = kafka
kafka.bootstrap.servers = localhost:9092
```

## task-canal-adapter

[canal.adapter](https://github.com/alibaba/canal/releases/download/canal-1.1.6/canal.adapter-1.1.6.tar.gz)模块作用:
同步依赖调度平台的元数据表到compass，只同步任务相关和用户表，其他表按需同步

例如对于DolphinScheduler： t_ds_project.yml 定义同步了 ds_project表，若需要同步其他表可参考conf/rdb下配置

项目中已提供DolphinScheduler和Airflow平台同步模板，若使用其他平台可参考模板

```
task-canal-adapter
├── bin
│   ├── compass_env.sh
│   ├── init_canal_adapter.sh       下载canal.adapter压缩包和解压相关lib和plugin
│   ├── restart.sh
│   ├── startup.sh
│   └── stop.sh
├── canal.adapter-1.1.6.tar.gz      compass不提供canal依赖包，可通过init_canal.sh下载，若无网络则自行下载到task-canal-adapter根目录
├── conf
│   ├── application.yml
│   └── rdb
│       ├── airflow_db_ab_user.yml
│       ├── airflow_db_dag_run.yml
│       ├── airflow_db_dag.yml
│       ├── airflow_db_task_instance.yml
│       ├── t_ds_process_definition.yml
│       ├── t_ds_process_instance.yml
│       ├── t_ds_process_task_relation.yml
│       ├── t_ds_project.yml
│       ├── t_ds_task_definition.yml
│       ├── t_ds_task_instance.yml
│       ├── t_ds_user.yml
│       └── template.yml
├── lib
└── plugin
```

### 表数据全量同步接口

示例：curl "localhost:8181/etl/rdb/mysql1/template.yml" -X POST

其中template.yml即为conf/rdb下的配置文件

### 核心配置

conf/application.yml

```
canal.conf:
  srcDataSources:
    defaultDS:
      # Scheduling platform MySQL synchronization account
      url: ${CANAL_ADAPTER_SOURCE_DATASOURCE_URL}
      username: ${CANAL_ADAPTER_SOURCE_DATASOURCE_USERNAME}
      password: ${CANAL_ADAPTER_SOURCE_DATASOURCE_PASSWORD}

  canalAdapters:
  - instance: mysqldata # kafka topic
    groups:
    - groupId: g1
      outerAdapters:
      - name: rdb
        key: mysql1
        properties:
          # Compass platform datasource account
          jdbc.url: ${CANAL_ADAPTER_DESTINATION_DATASOURCE_URL}
          jdbc.username: ${CANAL_ADAPTER_DESTINATION_DATASOURCE_USERNAME}
          jdbc.password: ${CANAL_ADAPTER_DESTINATION_DATASOURCE_PASSWORD}
```

conf/rdb/template.yml

```
dataSourceKey: defaultDS
destination: mysqldata
groupId: g1
outerAdapterKey: mysql1
concurrent: false
dbMapping:
  database: ${SCHEDULER_MYSQL_DB}
  # 调度平台MySQL表
  table: example
  # compass平台MySQL表
  targetTable: example
  # 主键配置
  targetPk:
    id: id
  mapAll: true
  commitBatch: 1
```

## task-syncer

task-syncer模块是关联调度平台和compass的抽象层，使得compass能够兼容和诊断不同的调度平台任务，该模块抽象定义了compass核心依赖的关系表：

user：登录和权限校验，隔离不同用户权限

project：项目关系

flow：工作流定义关系

task：具体任务定义关系

task_instance：任务运行实例

其中关系是user -> project -> flow -> task -> task_instance，可根据实际调度平台自行定义关系

```
task-syncer
├── bin
│   ├── compass_env.sh
│   ├── startup.sh
│   └── stop.sh
├── conf
│   ├── application-airflow.yml
│   ├── application-dolphinscheduler.yml
│   ├── application.yml
│   └── logback.xml
├── lib
```

### 核心配置

conf/application-xxx.yml定义了数据同步表字段之间的映射关系，实现源表和目标表的转化，

columnMapping 实现了字段之间的映射

columnValueMapping 实现了字段值的映射

constantColumn 实现了常量列的映射

columnDep 实现了列字段值依赖查询，可自定义SQL实现表字段之间的关联

下面以同步DolphinScheduler调度平台示例说明

user表映射：

```
# DolphinScheduler库名
- schema: "dolphinscheduler" 
  # DolphinScheduler user表
  table: "t_ds_user"
  # compass user表        
  targetTable: "user"
  # columnMapping用于字段映射，key是compass定义的字段，值是DolphinScheduler定义的字段   
  columnMapping: 
    user_id: "id"           
    username: "user_name"
    password: "user_password"
    is_admin: "user_type"
    email: "email"
    phone: "phone"
    create_time: "create_time"
    update_time: "update_time"
  # 字段值映射, 目标字段值, 源字段值, 字段类型
  columnValueMapping:
    is_admin: [ { targetValue: "0", originValue: [ "0" ] }, { targetValue: "1", originValue: [ "1" ] } ]
  # 常量列定义
  constantColumn:
    scheduler_type: "DolphinScheduler"
```

task_instance表映射：

```
- schema: "dolphinscheduler"
  table: "t_ds_task_instance"
  targetTable: "task_instance"
  columnMapping:
    id: "id"
    project_name: ""
    flow_name: ""
    task_name: "name"
    start_time: "start_time"
    end_time: "end_time"
    execution_time: ""
    task_state: "state"
    task_type: "task_type"
    retry_times: "retry_times"
    max_retry_times: "max_retry_times"
    worker_group: "worker_group"
    create_time: "create_time"
    update_time: "update_time"
  columnValueMapping:
    task_state:
      - { targetValue: "success", originValue: [ "7", "14" ] }
      - { targetValue: "fail", originValue: [ "6", "9" ] } 
      - { targetValue: "other", originValue: [ "0", "1", "2", "3", "4", "5", "8", "10", "11", "12", "13" ] }
  columnDep:
    # 列字段值依赖，由于该表缺失了project_name, flow_name, execution_time字段，因此需要关联其他表查询
    columns: [ "project_name", "flow_name", "execution_time" ]
    queries: [ "select t2.schedule_time as execution_time, t3.name as flow_name, t4.name as project_name from t_ds_task_instance as t1 inner join t_ds_process_instance as t2 on t1.process_instance_id = t2.id inner join t_ds_process_definition as t3 on t2.process_definition_code = t3.code inner join t_ds_project as t4 on t3.project_code=t4.code where t1.id=${id}" ]
```

## task-collector

task-collector模块整合了数据采集职责：关联task_name、applicationId、hdfs_log_path（原task-application功能），并定时同步Yarn ResourceManager、Spark HistoryServer App元数据（原task-metadata功能）。

```
task-collector/
├── bin
│   ├── compass_env.sh
│   ├── startup.sh
│   └── stop.sh
├── conf
│   ├── application-airflow.yml
│   ├── application-dolphinscheduler.yml
│   ├── application-hadoop.yml
│   ├── application.yml
│   └── logback.xml
├── lib
```

### 配置

conf/application-hadoop.yml — HDFS namenode及Yarn/Spark集群配置：

```
hadoop:
  namenodes:
    - nameservices: logs-hdfs
      namenodesAddr: [ "host1", "host2" ]
      namenodes: ["namenode1", "namenode2"]
      user: hdfs
      password:
      port: 8020
      matchPathKeys: [ "flume" ]

  yarn:
    - clusterName: "bigdata"
      resourceManager: [ "ip:port" ]
      jobHistoryServer: "ip:port"

  spark:
    sparkHistoryServer: [ "ip:port" ]
```

`conf/application-dolphinscheduler/airflow/custom.yml` — 日志路径拼接规则，用于从调度日志中提取applicationId：

```
custom:
  rules:
    - logPathDep:
        query: "select CASE WHEN end_time IS NOT NULL THEN DATE_ADD(end_time, INTERVAL 1 second) ELSE start_time END as end_time,log_path from t_ds_task_instance where id=${id}"
      logPathJoins:
        - { "column": "", "data": "/flume/dolphinscheduler" }
        - { "column": "end_time", "regex": "^.*(?<date>\\d{4}-\\d{2}-\\d{2}).+$", "name": "date" }
        - { "column": "log_path", "regex": "^.*logs/(?<logpath>.*)$", "name": "logpath" }
      extractLog:
        regex: "^.*Submitted application (?<applicationId>application_[0-9]+_[0-9]+).*$"
        name: "applicationId"
```

## task-analyzer

task-analyzer模块整合了工作流层异常检测（原task-detect）和引擎层日志解析诊断（原task-parser）。支持：运行失败、基线耗时异常、SQL失败、Shuffle失败、内存溢出、CPU/内存浪费、数据倾斜等Spark/MR异常。

```
task-analyzer/
├── bin
│   ├── compass_env.sh
│   ├── startup.sh
│   └── stop.sh
├── conf
│   ├── application-hadoop.yml
│   ├── application.yml
│   ├── logback.xml
│   ├── rules.json
│   └── scripts
│       └── logRecordConsumer.lua
├── lib
```

### 配置

conf/rules.json — 日志解析规则，支持scheduler/driver/executor/yarn日志类型。

conf/application.yml — 检测规则及Spark事件日志检测器：

```
custom:
  detectionRule:
    durationWarning: 2      # 单位：小时
    alwaysFailedWarning: 10 # 单位：天
  detector:
    sparkEnvironmentConfig:
      sparkProperties:
        - spark.driver.memoryOverhead
        - spark.driver.memory
        - spark.executor.memoryOverhead
        - spark.executor.memory
        - spark.executor.cores
        - spark.dynamicAllocation.maxExecutors
        - spark.sql.shuffle.partitions
     ...
```


## task-gpt

task-gpt模块用于聚合日志模板，并使用chatgpt给模板解决方案

### 核心配置

conf/application.yml
```
chatgpt:
  enable: true
  apiKeys: "sk-xxx1,sk-xxx2"
  proxy: "https://proxy"
  model: "gpt-3.5-turbo"
  prompt: "你是一位资深大数据专家，教导初始者，我会给你一些异常，你将提供异常的解决方案"
```


## task-portal 与 task-ui

task-portal 与 task-ui 可视化前后端模块，提供诊断建议、报告总览、一键诊断、任务运行、APP运行、白名单等服务

task-ui前端默认一起编译放在task-portal/portal目录下

如果您需要单独部署前端，需要修改 task-ui/.env.production 下 VITE_APP_PROD_BACKEND，指定您的后端地址或者域名即可

web ui默认路径: http://localhost:7075/compass/

swagger ui默认路径：http://localhost:7075/compass/swagger-ui/index.html

关于用户和密码问题：

如果您是使用DolphinScheduler或Airflow调度平台，即compass_env.sh中配置export SCHEDULER="dolphinscheduler / airflow"时，账号密码和调度平台相同（需要已经同步数据）

如果您是自研调度或者测试，请设置 compass_env.sh 中 export SCHEDULER="custom"，执行 document/sql/compass.sql 之后，默认账密是compass,compass，该模式没进行账号密码校验，请注意数据安全

```
task-portal
├── bin
│   ├── compass_env.sh
│   ├── startup.sh
│   └── stop.sh
├── conf
├── lib
├── portal
│   ├── assets
│   └── index.html

```

## 离线任务上报元数据诊断

支持第三方上报Spark/MapReduce任务application元数据进行诊断，如果不需要同步调度平台元数据和日志，只要启动task-portal和task-analyzer模块。

请求接口：http://[compass_host]/compass/openapi/offline/app/metadata

请求方式:  POST

参数类型 ：JSON

元数据来自http://rm-http-address:port/ws/v1/cluster/apps

| 参数名称                           | 类型     | 是否必填          | 描述                         |
|:-------------------------------|:-------|:--------------|:---------------------------|
| applicationId                  | String | 是             | YARN的applicationid         |
| applicationType                | String | 是             | YARN的任务类型：SPARK或者MAPREDUCE |
| vcoreSeconds                   | Double | 是             | YARN的vcoreSeconds          |
| memorySeconds                  | Double | 是             | YARN的memorySeconds         |
| startedTime                    | Long   | 是             | YARN的startedTime           |
| finishedTime                   | Long   | 是             | YARN的finishedTime          |
| elapsedTime                    | Double | 是             | YARN的elapsedTime           |
| amHostHttpAddress              | String | 是             | YARN的amHostHttpAddress     |
| sparkEventLogFile              | String | SPARK任务必填     | SparkEventLog绝对路径          |
| sparkExecutorLogDirectory      | String | SPARK任务必填     | 到applicationid层级目录         |
| mapreduceEventLogDirectory     | String | MAPREDUCE任务必填 | 到日期层级前缀目录                  |
| mapreduceContainerLogDirectory | String | MAPREDUCE任务必填 | 到applicationid层级目录         |
| diagnostics                    | String | 否             | YARN的diagnostics           |
| queue                          | String | 否             | YARN的queue                 |
| user                           | String | 否             | YARN的user                  |
| clusterName                    | String | 否             | 集群名称                       |

请求参数示例：
```json
{
    "applicationId": "application_1673850090992_30536",
    "applicationType": "SPARK",
    "vcoreSeconds": 550,
    "memorySeconds": 77079,
    "startedTime": 1692611101256,
    "finishedTime": 1692617022920,
    "elapsedTime": 35419,
    "amHostHttpAddress": "dgtest01:8043",
    "sparkEventLogFile": "hdfs://logs-cluster/user/spark/applicationHistory/application_1673850090992_30536_1",
    "sparkExecutorLogDirectory": "hdfs://logs-cluster/tmp/logs/hdfs/logs/application_1673850090992_30536",
    "mapreduceEventLogDirectory": "hdfs://logs-cluster/tmp/hadoop-yarn/staging/history/done", // MAPREDUCE任务必填
    "mapreduceContainerLogDirectory": "hdfs://logs-cluster/tmp/logs/root/logs/application_1673850090992_30536", // MAPREDUCE任务必填
    "diagnostics": "",
    "queue": "root",
    "user": "root",
    "clusterName": "test"
}
```


## 一键诊断功能

离线诊断支持全量(包含非调度平台提交任务)Spark/MapReduce任务进行一键诊断，如果仅需要体验该功能，只要启动task-portal、task-collector和task-analyzer模块。
