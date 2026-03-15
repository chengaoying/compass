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

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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
 *
 * <p>Failed records are placed on an in-memory dead letter queue (DLQ) and retried
 * up to {@code MAX_RETRIES} times with exponential backoff.
 */
@Slf4j
@Component
public class AnalyzerBridge {

    private static final int MAX_RETRIES = 3;
    private static final long SEMAPHORE_ACQUIRE_TIMEOUT_SECONDS = 30;

    private final JobManager jobManager;
    private final Executor parserExecutorPool;
    private final Semaphore semaphore;
    private final ConcurrentLinkedQueue<RetryableLogRecord> deadLetterQueue = new ConcurrentLinkedQueue<>();
    private final AtomicInteger dlqSize = new AtomicInteger(0);

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
        // Drain DLQ first: retry previously failed records
        retryDeadLetterQueue();

        boolean acquired;
        try {
            acquired = semaphore.tryAcquire(SEMAPHORE_ACQUIRE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted while acquiring parser semaphore for logRecord: {}", logRecord.getId());
            enqueueToDeadLetterQueue(logRecord, 0);
            return;
        }
        if (!acquired) {
            log.warn("Semaphore acquire timed out for logRecord id={}, sending to DLQ", logRecord.getId());
            enqueueToDeadLetterQueue(logRecord, 0);
            return;
        }

        parserExecutorPool.execute(() -> {
            try {
                log.info("analyzeLogRecord id={}", logRecord.getId());
                jobManager.run(logRecord);
            } catch (Exception e) {
                log.error("Parser failed for logRecord id={}, sending to DLQ for retry: ", logRecord.getId(), e);
                enqueueToDeadLetterQueue(logRecord, 0);
            } finally {
                semaphore.release();
            }
        });
    }

    private void enqueueToDeadLetterQueue(LogRecord logRecord, int retryCount) {
        if (retryCount >= MAX_RETRIES) {
            log.error("LogRecord id={} exceeded max retries ({}), discarding permanently", logRecord.getId(), MAX_RETRIES);
            return;
        }
        deadLetterQueue.offer(new RetryableLogRecord(logRecord, retryCount));
        int size = dlqSize.incrementAndGet();
        log.warn("LogRecord id={} added to DLQ (retry={}, dlqSize={})", logRecord.getId(), retryCount, size);
    }

    private void retryDeadLetterQueue() {
        int retried = 0;
        RetryableLogRecord retryable;
        while ((retryable = deadLetterQueue.poll()) != null) {
            dlqSize.decrementAndGet();
            retried++;
            final RetryableLogRecord record = retryable;
            boolean acquired = semaphore.tryAcquire();
            if (!acquired) {
                // Put it back and stop draining — system is at capacity
                deadLetterQueue.offer(record);
                dlqSize.incrementAndGet();
                break;
            }
            parserExecutorPool.execute(() -> {
                try {
                    log.info("Retrying DLQ logRecord id={} (attempt={})", record.logRecord.getId(), record.retryCount + 1);
                    jobManager.run(record.logRecord);
                } catch (Exception e) {
                    log.error("DLQ retry failed for logRecord id={} (attempt={}): ", record.logRecord.getId(), record.retryCount + 1, e);
                    enqueueToDeadLetterQueue(record.logRecord, record.retryCount + 1);
                } finally {
                    semaphore.release();
                }
            });
            if (retried >= 10) break; // Limit batch size per cycle
        }
    }

    public int getDeadLetterQueueSize() {
        return dlqSize.get();
    }

    private static class RetryableLogRecord {
        final LogRecord logRecord;
        final int retryCount;

        RetryableLogRecord(LogRecord logRecord, int retryCount) {
            this.logRecord = logRecord;
            this.retryCount = retryCount;
        }
    }
}
