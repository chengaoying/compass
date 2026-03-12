#!/bin/bash
# Minimal debug startup script
# Only starts: task-portal, task-collector, task-analyzer
# Enables JPDA remote debugging on ports 5005/5006/5007

HOME_DIR=$(cd $(dirname $0)/.. && pwd)
ENV_SH=$(dirname $0)/compass_env.sh

if [ -f ${ENV_SH} ]; then
  source ${ENV_SH}
  for dir in ${HOME_DIR}/task-portal ${HOME_DIR}/task-collector ${HOME_DIR}/task-analyzer; do
    if [ -d $dir ]; then
      cp ${ENV_SH} $dir/bin/
    fi
  done
fi

# Copy hadoop conf to modules that need it
for dir in task-collector task-analyzer; do
  if [ -d ${HOME_DIR}/${dir}/conf ] && [ -f ${HOME_DIR}/conf/application-hadoop.yml ]; then
    cp ${HOME_DIR}/conf/application-hadoop.yml ${HOME_DIR}/${dir}/conf/
  fi
done

# Wait for dependencies to be ready
echo "Waiting for PostgreSQL..."
retry -t 30 -d 2 -- bash -c "echo 'SELECT 1' | busybox nc -w 1 postgres 5432 >/dev/null 2>&1" 2>/dev/null || echo "PostgreSQL may not be ready, continuing..."

echo "Waiting for Kafka..."
retry -t 15 -d 2 -- bash -c "busybox nc -w 1 kafka 9092 >/dev/null 2>&1" 2>/dev/null || echo "Kafka may not be ready, continuing..."

echo "Waiting for Redis..."
retry -t 15 -d 2 -- bash -c "busybox nc -w 1 redis 6379 >/dev/null 2>&1" 2>/dev/null || echo "Redis may not be ready, continuing..."

echo "Waiting for OpenSearch..."
retry -t 30 -d 2 -- bash -c "busybox nc -w 1 opensearch 9200 >/dev/null 2>&1" 2>/dev/null || echo "OpenSearch may not be ready, continuing..."

echo ""
echo "============================================"
echo " Starting Compass (Minimal Debug Mode)"
echo "============================================"

# Debug JVM options (JPDA remote debug, suspend=n so services start immediately)
JAVA_DEBUG_OPTS_PORTAL="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:${JAVA_DEBUG_PORT_PORTAL:-5005}"
JAVA_DEBUG_OPTS_COLLECTOR="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:${JAVA_DEBUG_PORT_COLLECTOR:-5006}"
JAVA_DEBUG_OPTS_ANALYZER="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:${JAVA_DEBUG_PORT_ANALYZER:-5007}"

JAVA_OPTS_COMMON="-Djava.awt.headless=true -Djava.net.preferIPv4Stack=true -Dfile.encoding=UTF-8 -Djava.security.egd=file:/dev/./urandom"
JAVA_OPTS_GC="-server -XX:+UseG1GC -XX:G1HeapRegionSize=8m"

start_service() {
  local name=$1
  local main_class=$2
  local debug_opts=$3
  local dir=${HOME_DIR}/${name}

  if [ ! -d ${dir} ]; then
    echo "[SKIP] ${name}: directory not found"
    return
  fi

  mkdir -p ${dir}/logs

  echo "[START] ${name} (debug: ${debug_opts})"
  nohup java \
    -DappName=${name} \
    ${JAVA_OPTS_COMMON} \
    ${JAVA_OPTS_GC} \
    ${debug_opts} \
    -cp "${dir}/conf":"${dir}/lib/*" \
    ${main_class} \
    >${dir}/logs/stdout.log 2>&1 &

  local pid=$!
  echo $pid > ${dir}/tpid
  echo "[OK] ${name} started (pid=${pid})"
}

# Start only the 3 core services
start_service "task-portal"    "com.oppo.cloud.portal.TaskPortalApplication"       "${JAVA_DEBUG_OPTS_PORTAL}"
start_service "task-collector" "com.oppo.cloud.collector.TaskCollectorApplication"  "${JAVA_DEBUG_OPTS_COLLECTOR}"
start_service "task-analyzer"  "com.oppo.cloud.analyzer.TaskAnalyzerApplication"   "${JAVA_DEBUG_OPTS_ANALYZER}"

echo ""
echo "============================================"
echo " Compass Debug Mode Started"
echo "--------------------------------------------"
echo " Web UI:          http://localhost:7075/compass"
echo " Login:           compass / compass"
echo "--------------------------------------------"
echo " Debug Ports (JPDA):"
echo "   task-portal:    5005"
echo "   task-collector: 5006"
echo "   task-analyzer:  5007"
echo "============================================"
