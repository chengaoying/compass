# Compass Module Introduction

## Workspaces

```
compass
├── bin
│   ├── compass_env.sh                  global variable configuration
│   ├── start_all.sh                    start script
│   └── stop_all.sh                     stop script
├── conf
│   └── application-hadoop.yml          hadoop configuration
├── task-collector                      Collect scheduler task instances, associate ApplicationId and HDFS log paths, sync Yarn/Spark metadata
├── task-analyzer                       Workflow anomaly detection and engine-layer log parsing (Spark/MR diagnosis)
├── task-canal                          Synchronize scheduler metadata to Compass as a diagnostic event.
├── task-canal-adapter                  Synchronize scheduler metadata to Compass, save the original table, and perform data-assisted queries.
├── task-portal                         Display diagnostic and analytical results for Spark, Flink, MapReduce, and the scheduler.
├── task-flink                          Flink resource diagnostic module
├── task-flink-core                     Flink diagnostic rules
├── task-gpt                            Aggregate log templates and use ChatGPT to provide solutions for the templates.
└── task-syncer                         Synchronize scheduler metadata to Compass.

```
### Historical data synchronization

If the scheduling platform database is MySQL and the Compass database is PostgreSQL, you can use [pgloader](https://github.com/dimitri/pgloader) to create dependent tables and synchronize historical full data.

Synchronize dolphinscheduler table:
```
LOAD DATABASE
     FROM mysql://root:password@localhost:3306/dolphinscheduler
     INTO postgresql://postgres@localhost:5432/compass
     ALTER SCHEMA 'dolphinscheduler' RENAME TO 'public'
     INCLUDING ONLY TABLE NAMES MATCHING 't_ds_process_definition','t_ds_process_instance','t_ds_process_task_relation','t_ds_project','t_ds_task_definition','t_ds_task_instance','t_ds_user';
```

Synchronize airflow table:
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

If the scheduling platform database is MySQL and the Compass database is MySQL, you can use task-canal-adapter synchronize historical full data.

Synchronize dolphinscheduler table:

curl "localhost:8181/etl/rdb/mysql1/t_ds_process_definition.yml" -X POST

curl "localhost:8181/etl/rdb/mysql1/t_ds_process_instance.yml" -X POST

curl "localhost:8181/etl/rdb/mysql1/t_ds_process_task_relation.yml" -X POST

curl "localhost:8181/etl/rdb/mysql1/t_ds_project.yml" -X POST

curl "localhost:8181/etl/rdb/mysql1/t_ds_task_definition.yml" -X POST

curl "localhost:8181/etl/rdb/mysql1/t_ds_task_instance.yml" -X POST

curl "localhost:8181/etl/rdb/mysql1/t_ds_user.yml" -X POST

Synchronize airflow table:

curl "localhost:8181/etl/rdb/mysql1/airflow_db_ab_user.yml" -X POST

curl "localhost:8181/etl/rdb/mysql1/airflow_db_dag_run.yml" -X POST

curl "localhost:8181/etl/rdb/mysql1/airflow_db_dag.yml" -X POST

curl "localhost:8181/etl/rdb/mysql1/airflow_db_task_instance.yml" -X POST

## task-canal

If you are using [DolphinScheduler](https://github.com/apache/dolphinscheduler)
or [Airflow](https://github.com/apache/airflow) or self-developed scheduling platform,
Metadata is stored in MySQL，use [canal.deployer](https://github.com/alibaba/canal/releases/download/canal-1.1.6/canal.deployer-1.1.6.tar.gz)
to subscribe to MySQL binlog and synchronize to Kafka. The default topic is 'mysqldata'.

```
task-canal
├── bin
│   ├── compass_env.sh           compass global variable
│   ├── init_canal.sh            dwonload canal.deployer dependency
│   ├── restart.sh         
│   ├── startup.sh
│   └── stop.sh
├── canal.deployer-1.1.6.tar.gz   Compass does not provide the Canal dependency package, which can be downloaded through init_canal.sh. If there is no network, it can be downloaded to the root directory of task-canal manually.
├── conf
│   ├── example
│   │   ├── instance.properties   Source table configuration
│   ├── canal_local.properties    zk,kafka configuration
│   ├── canal.properties
│   ├── logback.xml
├── lib
└── plugin
```

### core configuration

conf/example/instance.properties

```
canal.instance.master.address=localhost:33066
canal.instance.dbUsername=root
canal.instance.dbPassword=root
canal.instance.filter.regex=.*\\..*
canal.mq.topic=mysqldata

# Dynamic topics and partitions are not configured by default.
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

[canal.adapter](https://github.com/alibaba/canal/releases/download/canal-1.1.6/canal.adapter-1.1.6.tar.gz):
Synchronize metadata tables from the scheduling platform to Compass, and only synchronize the task and user tables. Other tables can be synchronized as needed.

For example, for DolphinScheduler: the t_ds_project.yml defines the synchronization of the `ds_project` table. If you need to synchronize other tables, you can refer to the configuration in `conf/rdb`

The project has provided synchronization templates for both DolphinScheduler and Airflow platforms. If you are using other platforms, you can refer to these templates.
```
task-canal-adapter
├── bin
│   ├── compass_env.sh
│   ├── init_canal_adapter.sh       Download the Canal.adapter package and extract the relevant lib and plugin files.
│   ├── restart.sh
│   ├── startup.sh
│   └── stop.sh
├── canal.adapter-1.1.6.tar.gz       Compass does not provide the Canal dependency package, which can be downloaded through init_canal.sh. If there is no network, it can be downloaded to the root directory of task-canal-adapter manually.
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

### Full table data synchronization interface

For example：curl "localhost:8181/etl/rdb/mysql1/template.yml" -X POST

The `template.yml` is the configuration file under `conf/rdb`.

### Configuration

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
  # Scheduling platform MySQL table
  table: example
  # Compass platform MySQL table
  targetTable: example
  # Primary key configuration
  targetPk:
    id: id
  mapAll: true
  commitBatch: 1
```

## task-syncer

The task-syncer module is the abstraction layer that connects the scheduling platform and Compass, enabling Compass to be compatible with and diagnose different scheduling platform tasks. This module abstractly defines the relationship table that is the core dependency of Compass:

user：Login and permission verification, isolating different user permissions

project：Project definition

flow：Workflow definition 

task：Task definition

task_instance：Task running instance

The relationship: user -> project -> flow -> task -> task_instance, relationships can be defined according to the actual scheduling platform.

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

### Configuration

`conf/application-xxx.yml` defines the mapping relationship between the fields of data synchronization tables, which achieves the conversion between source tables and target tables,

`columnMapping` achieves the mapping between fields.

`columnValueMapping` achieves the mapping of field values.

`constantColumn` achieves the mapping of constant columns.

`columnDep` implements the column value dependency query and allows custom SQL to establish associations between table columns.

Below is an example of synchronizing DolphinScheduler scheduling platform:

user table：

```
# DolphinScheduler schema
- schema: "dolphinscheduler" 
  # DolphinScheduler user table
  table: "t_ds_user"
  # compass user table       
  targetTable: "user"
  # columnMapping is used for field mapping, where the key is the field defined by Compass, and the value is the field defined by DolphinScheduler.
  columnMapping: 
    user_id: "id"           
    username: "user_name"
    password: "user_password"
    is_admin: "user_type"
    email: "email"
    phone: "phone"
    create_time: "create_time"
    update_time: "update_time"
  # Field value mapping, target field value, source field value, field type.
  columnValueMapping:
    is_admin: [ { targetValue: "0", originValue: [ "0" ] }, { targetValue: "1", originValue: [ "1" ] } ]
  # Constant column definition
  constantColumn:
    scheduler_type: "DolphinScheduler"
```

task_instance table：

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
    # Column field value dependency, since this table lacks the fields project_name, flow_name, execution_time, it needs to be queried in association with other tables.
    columns: [ "project_name", "flow_name", "execution_time" ]
    queries: [ "select t2.schedule_time as execution_time, t3.name as flow_name, t4.name as project_name from t_ds_task_instance as t1 inner join t_ds_process_instance as t2 on t1.process_instance_id = t2.id inner join t_ds_process_definition as t3 on t2.process_definition_code = t3.code inner join t_ds_project as t4 on t3.project_code=t4.code where t1.id=${id}" ]
```

## task-collector

The task-collector module consolidates data collection responsibilities: it associates task_name, applicationId, and hdfs_log_path from scheduler logs (formerly task-application), and periodically synchronizes Yarn ResourceManager and Spark HistoryServer App metadata (formerly task-metadata).

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

### Configuration

conf/application-hadoop.yml — HDFS namenode and Yarn/Spark cluster configuration:

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

`conf/application-dolphinscheduler/airflow/custom.yml` — log path assembly rules for extracting applicationId from scheduler logs. The scheduler worker log_path (e.g. `/home/.../logs/xxx/33552/33934.log`) is mapped to the HDFS path (e.g. `hdfs://log-hdfs:8020/flume/dolphinscheduler/2023-03-30/xxx/33552/...`):

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

Note: The native Flume-taildir-source plugin does not support recursively traversing subdirectory files and requires modification.

## task-analyzer

The task-analyzer module consolidates both workflow-level anomaly detection (formerly task-detect) and engine-layer log parsing and diagnosis (formerly task-parser). It handles: running failure, baseline exceptions, SQL failure, Shuffle failure, memory overflow, CPU/memory waste, data skewness, and other Spark/MR anomalies.

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

### Configuration

The `conf/rules.json` configuration is used to write log parsing rules.
The fields are defined as following:

**logType**: scheduler/driver/executor/yarn

**action**: Define the name of each matching rule.

**desc**: Description for action

**category**: Definition of rule types, such as shuffleFailed/sqlFailed, etc.

**step**: order of matching action.

**parserType**: Match type, DEFAULT(match by rows or blocks), JOIN(Merge results into one row before matching.)

**parserTemplate**: Text parsing templates consist of the first line, middle lines, and ending lines.

For example:
```
  {
    "logType": "scheduler",
    "actions": [
      {
        "action": "otherError",
        "desc": "other error info",
        "category": "otherException",
        "step": 1,
        "skip": false,
        "parserType": "DEFAULT",
        "parserTemplate": {
          "heads": [ "^.+ERROR.+$" ],
          "middles": [],
          "tails": []
        },
        "groupNames": [],
        "children": []
      }
    ]
  }
```

`conf/application.yml` — detection rules and Spark event log detectors:

```
custom:
  detectionRule:
    durationWarning: 2      # unit: hour
    alwaysFailedWarning: 10 # unit: day
  detector:
    sparkEnvironmentConfig:
      sparkProperties:
        - spark.driver.memoryOverhead
        - spark.driver.memory
        - spark.executor.memoryOverhead
        - spark.executor.memory
        - spark.executor.cores
        - spark.dynamicAllocation.maxExecutors
        - spark.default.parallelism
        - spark.sql.shuffle.partitions
     ...
```


## task-gpt

The task-gpt module is used to aggregate log templates and use ChatGPT to provide solutions for the templates.

### Configuration

conf/application.yml
```
chatgpt:
  enable: true
  apiKeys: "sk-xxx1,sk-xxx2"
  proxy: "https://proxy"
  model: "gpt-3.5-turbo"
  prompt: "You are a senior expert in big data, teaching beginners. I will give you some anomalies and you will provide solutions to them."
```


## task-portal and task-ui

**task-portal** and **task-ui** are visual front-end and back-end modules that provide services such as diagnostic recommendations, report overviews, one-click diagnosis, task execution, APP execution, and whitelist.

The task-ui front-end is compiled together by default and placed in the task-portal/portal directory.

If you need to deploy the front-end separately, you need to modify VITE_APP_PROD_BACKEND in task-ui/.env.production to specify your backend address or domain name.

WebUI default http path: http://localhost:7075/compass/

SwaggerUI default http path：http://localhost:7075/compass/swagger-ui/index.html

About the **username** and **password** ：

If you are using DolphinScheduler or Airflow scheduling platform, that is configured in compass_env.sh using 'export SCHEDULER="dolphinscheduler/airflow"', the account password will be the same as the scheduling platform (data synchronization is required).

If you are using a self-developed or testing scheduling platform, please set 'export SCHEDULER="custom"' in compass_env.sh. After executing document/sql/compass.sql, the default account and password are 'compass, compass'. This mode does not perform account and password verification. Please pay attention to data security.
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

## Offline task metadata reporting diagnosis.

Supports third-party reporting of Spark/MapReduce task application metadata for diagnosis. If you don't need to synchronize scheduling platform metadata and logs, simply start the task-portal and task-analyzer modules.

Request API：http://[compass_host]/compass/openapi/offline/app/metadata

Request Method:  POST

ContentType: application/json

Metadata comes from **http://rm-http-address:port/ws/v1/cluster/apps**

parameter				| data type		     | optional	            | description  
:----				|:---------|:-----------------|:---	
applicationId				| String		 | Yes			             | YARN application id
applicationType				| String		 | Yes			             | YARN App Type：SPARK or MAPREDUCE
vcoreSeconds			| Double		    | Yes			             | YARN vcoreSeconds
memorySeconds			| Double		   | Yes			             | YARN memorySeconds
startedTime			| Long		   | Yes			             | YARN startedTime
finishedTime			| Long		   | Yes			             | YARN finishedTime
elapsedTime			| Double		 | Yes			             | YARN elapsedTime
amHostHttpAddress			| String		 | Yes			             | YARN amHostHttpAddress
sparkEventLogFile			| String		 | Yes for SPARK			     | SparkEventLog absolutely log path
sparkExecutorLogDirectory			| String		 | Yes for SPARK			     | application id folder
mapreduceEventLogDirectory			| String		 | Yes for MAPREDUCE			 | folder 
mapreduceContainerLogDirectory			| String		 |Yes for MAPREDUCE			     | application id folder
diagnostics			| String		 | No			             | YARN diagnostics
queue			| String		 | No			             | YARN queue
user			| String		 | No			             | YARN user
clusterName			| String		 | No			             | cluster name

For example：
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
    "mapreduceEventLogDirectory": "hdfs://logs-cluster/tmp/hadoop-yarn/staging/history/done", // for MAPREDUCE
    "mapreduceContainerLogDirectory": "hdfs://logs-cluster/tmp/logs/root/logs/application_1673850090992_30536", // for MAPREDUCE
    "diagnostics": "",
    "queue": "root",
    "user": "root",
    "clusterName": "test"
}
```

## Task-flink (including task-core) 


## One-Click diagnosis

Offline diagnosis supports one-click diagnosis for all Spark/MapReduce tasks, including those not submitted to the scheduling platform. If you only want to experience this function, simply start the task-portal, task-collector, and task-analyzer modules.

