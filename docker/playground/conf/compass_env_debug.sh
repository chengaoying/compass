#!/bin/bash

# =============================================================================
# Minimal Debug Deployment Configuration
# Only essential services: portal, collector, analyzer
# Canal/Syncer/Flink/GPT disabled
# =============================================================================

# Scheduler type (not actively used in debug mode without Canal)
export SCHEDULER="custom"
export SPRING_PROFILES_ACTIVE="hadoop"

# Compass database (PostgreSQL)
export DATASOURCE_TYPE="postgresql"
export COMPASS_DATASOURCE_ADDRESS="postgres:5432"
export COMPASS_DATASOURCE_DB="compass"
export SPRING_DATASOURCE_URL="jdbc:postgresql://${COMPASS_DATASOURCE_ADDRESS}/${COMPASS_DATASOURCE_DB}"
export SPRING_DATASOURCE_USERNAME="postgres"
export SPRING_DATASOURCE_PASSWORD="postgres"

# Kafka (KRaft, single broker)
export SPRING_KAFKA_BOOTSTRAPSERVERS="kafka:9092"

# Redis (single node)
export REDIS_HOST="redis"
export REDIS_PORT="6379"
export SPRING_REDIS_CLUSTER_NODES="redis:6379"
export SPRING_REDIS_PASSWORD=""

# OpenSearch (single node, security disabled)
export SPRING_OPENSEARCH_NODES="opensearch:9200"
export SPRING_OPENSEARCH_USERNAME=""
export SPRING_OPENSEARCH_PASSWORD=""
export SPRING_OPENSEARCH_TRUSTSTORE=""
export SPRING_OPENSEARCH_TRUSTSTOREPASSWORD=""

# Spark compression codec (empty = no compression)
export SPARK_IO_COMPRESSION_CODEC=""

# ChatGPT disabled
export CHATGPT_ENABLE=false

# Canal disabled (no scheduler DB sync in debug mode)
export TASK_CANAL_ENABLE="False"

# =============================================================================
# Environment variables for new merged services
# =============================================================================
# task-collector (merged task-application + task-metadata)
export DB_URL="${SPRING_DATASOURCE_URL}"
export DB_USERNAME="${SPRING_DATASOURCE_USERNAME}"
export DB_PASSWORD="${SPRING_DATASOURCE_PASSWORD}"
export KAFKA_BOOTSTRAP_SERVERS="${SPRING_KAFKA_BOOTSTRAPSERVERS}"
export OPENSEARCH_NODES="${SPRING_OPENSEARCH_NODES}"
export OPENSEARCH_USERNAME="${SPRING_OPENSEARCH_USERNAME}"
export OPENSEARCH_PASSWORD="${SPRING_OPENSEARCH_PASSWORD}"

# JWT secret for portal (debug only, not for production)
export JWT_SECRET="compass-debug-secret-key-minimum-32-chars"
