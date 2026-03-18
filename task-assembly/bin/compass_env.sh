#!/bin/bash

# dolphinscheduler or airflow or custom
export SCHEDULER="dolphinscheduler"
export SPRING_PROFILES_ACTIVE="hadoop,${SCHEDULER}"

# Configuration for Scheduler MySQL, compass will subscribe data from scheduler database via canal
export SCHEDULER_MYSQL_ADDRESS="localhost:3306"
export SCHEDULER_MYSQL_DB="dolphinscheduler"
export SCHEDULER_DATASOURCE_URL="jdbc:mysql://${SCHEDULER_MYSQL_ADDRESS}/${SCHEDULER_MYSQL_DB}?useUnicode=true&characterEncoding=utf-8&serverTimezone=Asia/Shanghai"
export SCHEDULER_DATASOURCE_USERNAME=""
export SCHEDULER_DATASOURCE_PASSWORD=""

# Configuration for compass database(mysql or postgresql)
export DATASOURCE_TYPE="mysql"
export COMPASS_DATASOURCE_ADDRESS="localhost:3306"
export COMPASS_DATASOURCE_DB="compass"
export SPRING_DATASOURCE_URL="jdbc:${DATASOURCE_TYPE}://${COMPASS_DATASOURCE_ADDRESS}/${COMPASS_DATASOURCE_DB}"
export SPRING_DATASOURCE_USERNAME=""
export SPRING_DATASOURCE_PASSWORD=""

# Configuration for compass Kafka, used to subscribe data by canal and log queue, etc. (default version: 3.4.0)
export SPRING_KAFKA_BOOTSTRAPSERVERS="host1:port,host2:port"

# Configuration for compass redis, used to cache and log queue, etc . (cluster mode)
export SPRING_REDIS_CLUSTER_NODES="localhost:6379"
# Optional
export SPRING_REDIS_PASSWORD=""

# OpenSearch (default version: 1.3.12) or Elasticsearch (7.x~)
export SPRING_OPENSEARCH_NODES="localhost:9200"
# Optional
export SPRING_OPENSEARCH_USERNAME=""
# Optional
export SPRING_OPENSEARCH_PASSWORD=""
# Optional, needed by OpenSearch, keep empty if OpenSearch does not use truststore.
export SPRING_OPENSEARCH_TRUSTSTORE=""
# Optional, needed by OpenSearch, keep empty if OpenSearch does not use truststore.
export SPRING_OPENSEARCH_TRUSTSTOREPASSWORD=""

# spark.io.compression.codec: lz4/snappy/zstd (default: no compression)
export SPARK_IO_COMPRESSION_CODEC=""

#-----------------------------------------------------------------------------------
# The following export items will be automatically filled by the configuration above.
#-----------------------------------------------------------------------------------
# task-syncer
# source datasource
export SPRING_DATASOURCE_DYNAMIC_DATASOURCE_SOURCE_URL=${SCHEDULER_DATASOURCE_URL}
export SPRING_DATASOURCE_DYNAMIC_DATASOURCE_SOURCE_USERNAME=${SCHEDULER_DATASOURCE_USERNAME}
export SPRING_DATASOURCE_DYNAMIC_DATASOURCE_SOURCE_PASSWORD=${SCHEDULER_DATASOURCE_PASSWORD}
# destination datasource
export SPRING_DATASOURCE_DYNAMIC_DATASOURCE_DIAGNOSE_URL=${SPRING_DATASOURCE_URL}
export SPRING_DATASOURCE_DYNAMIC_DATASOURCE_DIAGNOSE_USERNAME=${SPRING_DATASOURCE_USERNAME}
export SPRING_DATASOURCE_DYNAMIC_DATASOURCE_DIAGNOSE_PASSWORD=${SPRING_DATASOURCE_PASSWORD}

