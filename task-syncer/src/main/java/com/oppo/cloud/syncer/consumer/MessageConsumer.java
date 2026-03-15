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

import com.alibaba.fastjson2.JSON;
import com.google.common.base.CaseFormat;
import com.oppo.cloud.syncer.config.DataSourceConfig;
import com.oppo.cloud.syncer.domain.Mapping;
import com.oppo.cloud.syncer.domain.RawTable;
import com.oppo.cloud.syncer.service.ActionService;
import com.oppo.cloud.syncer.service.impl.DummyService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Kafka consumer for Canal CDC messages.
 */
@Slf4j
@Component
public class MessageConsumer {

    @Resource
    private DataSourceConfig dataSourceConfig;

    @Autowired
    private Map<String, ActionService> serviceMap;

    /** Thread-safe table mapping, initialized once at startup */
    private volatile Map<String, List<Mapping>> tableMapping;

    @PostConstruct
    public void init() {
        initTableMapping();
    }

    /**
     * Receive and handle sql data
     */
    @KafkaListener(topics = "${spring.kafka.topics}", containerFactory = "kafkaListenerContainerFactory")
    public void receive(@Payload String message,
                        @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
                        @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                        Consumer consumer) {

        log.debug(String.format("From partition %d: %s", partition, message));

        // Parsing table data
        RawTable rawTable = JSON.parseObject(message, RawTable.class);

        List<Mapping> mappings = getTableMapping(rawTable.getTable());
        if (mappings == null || mappings.isEmpty()) {
            consumer.commitAsync();
            return;
        }

        boolean allSucceeded = true;
        for (Mapping mapping : mappings) {
            try {
                this.consumeMessage(rawTable, mapping);
            } catch (Exception e) {
                allSucceeded = false;
                log.error("Failed to process message for table={}, targetTable={}: {}",
                        rawTable.getTable(), mapping.getTargetTable(), e.getMessage(), e);
            }
        }

        if (allSucceeded) {
            consumer.commitAsync();
        } else {
            log.warn("Skipping commit for table={} due to processing errors, message will be redelivered",
                    rawTable.getTable());
        }
    }

    /**
     * Consuming data
     */
    public void consumeMessage(RawTable rawTable, Mapping mapping) {
        switch (rawTable.getType()) {
            case "INSERT":
                this.insertAction(rawTable, mapping);
                break;
            case "UPDATE":
                this.updateAction(rawTable, mapping);
                break;
            case "DELETE":
                this.deleteAction(rawTable, mapping);
                break;
            default: // ignore ...
                break;
        }
    }

    /**
     * Insert operation
     */
    public void insertAction(RawTable rawTable, Mapping mapping) {
        ActionService service = serviceMap.getOrDefault(serviceKey(mapping.getTargetTable()), new DummyService());
        service.insert(rawTable, mapping);
    }

    /**
     * Update operation
     */
    public void updateAction(RawTable rawTable, Mapping mapping) {
        ActionService service = serviceMap.getOrDefault(serviceKey(mapping.getTargetTable()), new DummyService());
        service.update(rawTable, mapping);
    }

    /**
     * Delete operation
     */
    public void deleteAction(RawTable rawTable, Mapping mapping) {
        ActionService service = serviceMap.getOrDefault(serviceKey(mapping.getTargetTable()), new DummyService());
        service.delete(rawTable, mapping);
    }

    /**
     * Get table mapping rules (lock-free)
     */
    public List<Mapping> getTableMapping(String table) {
        Map<String, List<Mapping>> mapping = this.tableMapping;
        if (mapping == null) {
            initTableMapping();
            mapping = this.tableMapping;
        }
        return mapping.getOrDefault(table, Collections.emptyList());
    }

    /**
     * Initialize table mapping rules using ConcurrentHashMap (thread-safe, no locking)
     */
    public void initTableMapping() {
        Map<String, List<Mapping>> newMapping = new ConcurrentHashMap<>();
        for (Mapping mapping : this.dataSourceConfig.getMappings()) {
            newMapping.computeIfAbsent(mapping.getTable(), k -> new CopyOnWriteArrayList<>()).add(mapping);
        }
        this.tableMapping = newMapping;
        log.info("Table mapping initialized with {} source tables", newMapping.size());
    }

    /**
     * Get service name: targetTable+Service
     * For example, userService=user_Service
     */
    public String serviceKey(String targetTable) {
        return CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, String.format("%s_Service", targetTable));
    }
}
