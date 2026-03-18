FROM eclipse-temurin:8-focal

RUN set -x && \
    ln -snf /usr/bin/bash /usr/bin/sh && \
    apt-get update -q && \
    apt-get install -y tzdata && \
    apt-get install -yq retry busybox && \
    rm -rf /var/lib/apt/lists/* && \
    mkdir /opt/busybox && \
    busybox --install /opt/busybox

ENV PATH=/opt/java/openjdk/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/opt/busybox

ENV TASK_CANAL_ENABLE="False"
ENV COMPASS_DEBUG="true"
ENV TZ=Asia/Shanghai

RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && \
    echo $TZ > /etc/timezone

COPY compass-*.tar.gz /opt

# Extract compass distribution
RUN tar -xzf /opt/compass-*.tar.gz -C /opt && \
    rm /opt/compass-*.tar.gz && \
    ln -s /opt/compass-* /opt/compass && \
    rm /opt/compass/bin/compass_env.sh

COPY conf/compass_env_debug.sh /opt/compass/bin/compass_env.sh
COPY conf/start_debug.sh /opt/compass/bin/start_debug.sh
RUN chmod +x /opt/compass/bin/start_debug.sh

# Expose service ports and debug ports
EXPOSE 7070 7071 7075 5005 5006 5007

CMD /opt/compass/bin/start_debug.sh && /bin/bash
