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

package com.oppo.cloud.analyzer.bridge;

import com.oppo.cloud.common.domain.job.LogRecord;
import com.oppo.cloud.parser.service.job.JobManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;

/**
 * AnalyzerBridge replaces the Redis queue handoff between task-detect and task-parser.
 *
 * <p>Previously, task-detect pushed {@link LogRecord} to a Redis List, and task-parser
 * polled that list every 2 seconds via a Lua script. This caused:
 * <ul>
 *   <li>2–5 second additional latency per task diagnosis</li>
 *   <li>Potential message loss on Redis restart (no persistence guarantee)</li>
 *   <li>Complex Lua scripts for distributed deduplication</li>
 *   <li>Requirement for Redis Cluster (3 nodes) just for this queue</li>
 * </ul>
 *
 * <p>After merging into task-analyzer, detection and parsing run in the same JVM.
 * The bridge directly calls {@link JobManager#run(LogRecord)} on a thread pool,
 * preserving concurrency control via a {@link Semaphore} (same logic as before).
 */
@Slf4j
@Component
public class AnalyzerBridge {

    private final JobManager jobManager;
    private final Executor parserExecutorPool;
    private final Semaphore semaphore;

    public AnalyzerBridge(JobManager jobManager,
                          Executor parserExecutorPool,
                          int maxConcurrentParsers) {
        this.jobManager = jobManager;
        this.parserExecutorPool = parserExecutorPool;
        this.semaphore = new Semaphore(maxConcurrentParsers);
    }

    /**
     * Asynchronously parse the log record.
     * Replaces: {@code redisService.lLeftPush(logRecordQueue, JSONObject.toJSONString(logRecord))}
     */
    public void analyze(LogRecord logRecord) {
        try {
            semaphore.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted while acquiring parser semaphore for logRecord: {}", logRecord.getId());
            return;
        }
        parserExecutorPool.execute(() -> {
            try {
                log.info("analyzeLogRecord id={}", logRecord.getId());
                jobManager.run(logRecord);
            } catch (Exception e) {
                log.error("Parser failed for logRecord id={}: ", logRecord.getId(), e);
            } finally {
                semaphore.release();
            }
        });
    }
}
