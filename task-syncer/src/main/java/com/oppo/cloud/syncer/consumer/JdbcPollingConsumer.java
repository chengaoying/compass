/*
 * Copyright 2023 OPPO.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.oppo.cloud.syncer.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.CaseFormat;
import com.oppo.cloud.common.domain.syncer.TableMessage;
import com.oppo.cloud.syncer.config.DataSourceConfig;
import com.oppo.cloud.syncer.domain.Mapping;
import com.oppo.cloud.syncer.producer.MessageProducer;
import com.oppo.cloud.syncer.service.ActionService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;

/**
 * JdbcPollingConsumer: Canal-free alternative for syncing scheduler metadata.
 *
 * <p>Instead of deploying Canal (canal-server + canal-adapter + ZooKeeper), this component
 * polls the scheduler database directly using timestamp-based incremental queries
 * (WHERE update_time > lastPolledTime). This covers the common case where the Compass
 * deployment can directly connect to the scheduler's database.
 *
 * <p>Activated when {@code custom.syncer.mode=jdbc} (default: {@code canal}).
 * The Canal-based {@link MessageConsumer} remains available for network-isolated deployments.
 *
 * <p>Poll interval: configurable via {@code custom.syncer.jdbc.pollIntervalMs} (default: 5000ms).
 *
 * <p>Idempotency: each polled record is processed only once because the poll window advances
 * monotonically. Duplicate processing on restart is bounded to the last poll window.
 */
@Slf4j
@Component
@ConditionalOnProperty(prefix = "custom.syncer", name = "mode", havingValue = "jdbc")
public class JdbcPollingConsumer {

    @Autowired
    private DataSourceConfig dataSourceConfig;

    @Autowired
    private Map<String, ActionService> serviceMap;

    @Autowired
    private MessageProducer messageProducer;

    @Autowired
    private ObjectMapper objectMapper;

    @Value("${spring.kafka.topic.taskInstance:task-instance}")
    private String taskInstanceTopic;

    /** Tracks the last successfully polled timestamp per scheduler table */
    private final Map<String, Date> lastPolledTime = new HashMap<>();

    /**
     * Poll all configured table mappings for new/updated records.
     * Runs every {@code custom.syncer.jdbc.pollIntervalMs} milliseconds.
     */
    @Scheduled(fixedDelayString = "${custom.syncer.jdbc.pollIntervalMs:5000}")
    public void poll() {
        if (dataSourceConfig.getMappings() == null) {
            return;
        }
        for (Mapping mapping : dataSourceConfig.getMappings()) {
            try {
                pollTable(mapping);
            } catch (Exception e) {
                log.error("Error polling table {}: ", mapping.getSourceTable(), e);
            }
        }
    }

    private void pollTable(Mapping mapping) throws Exception {
        String sourceTable = mapping.getSourceTable();
        Date since = lastPolledTime.getOrDefault(sourceTable,
                Date.from(LocalDateTime.now().minusHours(1).atZone(ZoneId.systemDefault()).toInstant()));

        // Use the scheduler's JdbcTemplate (dynamic datasource via DataSourceConfig)
        JdbcTemplate schedulerJdbc = new JdbcTemplate(
                dataSourceConfig.getSchedulerDataSource(mapping.getDataSource()));

        String updateTimeCol = mapping.getUpdateTimeColumn() != null
                ? mapping.getUpdateTimeColumn() : "update_time";

        String sql = "SELECT * FROM " + sourceTable + " WHERE " + updateTimeCol + " > ? ORDER BY " + updateTimeCol + " ASC";
        List<Map<String, Object>> rows = schedulerJdbc.queryForList(sql, since);

        if (rows.isEmpty()) {
            return;
        }

        log.info("Polled {} rows from {} since {}", rows.size(), sourceTable, since);
        Date maxUpdateTime = since;

        for (Map<String, Object> row : rows) {
            try {
                // Convert snake_case column names to camelCase (matches Canal output format)
                Map<String, Object> camelRow = new LinkedHashMap<>();
                for (Map.Entry<String, Object> entry : row.entrySet()) {
                    String camelKey = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, entry.getKey());
                    camelRow.put(camelKey, entry.getValue());
                }

                // Build a TableMessage in the same format as Canal produces
                TableMessage tableMessage = new TableMessage();
                tableMessage.setTable(sourceTable);
                tableMessage.setEventType("UPDATE");
                tableMessage.setBody(objectMapper.writeValueAsString(camelRow));

                // Route to the same ActionService that handles Canal messages
                String serviceKey = sourceTable;
                ActionService service = serviceMap.get(serviceKey);
                if (service != null) {
                    service.update(camelRow, mapping);
                }

                // Publish to task-instance Kafka topic (same as Canal path)
                messageProducer.sendMessage(taskInstanceTopic, objectMapper.writeValueAsString(tableMessage));

                // Track max update_time seen in this batch
                Object ut = row.get(updateTimeCol);
                if (ut instanceof Date && ((Date) ut).after(maxUpdateTime)) {
                    maxUpdateTime = (Date) ut;
                }
            } catch (Exception e) {
                log.error("Error processing row from {}: {}", sourceTable, row, e);
            }
        }

        lastPolledTime.put(sourceTable, maxUpdateTime);
    }
}
