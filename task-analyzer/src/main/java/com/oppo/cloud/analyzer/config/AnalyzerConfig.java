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

package com.oppo.cloud.analyzer.config;

import com.oppo.cloud.analyzer.bridge.AnalyzerBridge;
import com.oppo.cloud.parser.service.job.JobManager;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;

@Configuration
public class AnalyzerConfig {

    /** Thread pool for running workflow detectors (was task-detect's DETECT_EXECUTOR_POOL) */
    public static final String DETECT_EXECUTOR_POOL = "detectExecutorPool";

    /** Thread pool for running log parsers (was task-parser's REDIS_CONSUMER_THREAD_POOL) */
    public static final String PARSER_EXECUTOR_POOL = "parserExecutorPool";

    @Value("${custom.detect.threadPoolSize:10}")
    private int detectThreadPoolSize;

    @Value("${custom.parser.maxConcurrentParsers:10}")
    private int maxConcurrentParsers;

    @Bean(name = DETECT_EXECUTOR_POOL)
    public Executor detectExecutorPool() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(detectThreadPoolSize);
        executor.setMaxPoolSize(detectThreadPoolSize * 2);
        executor.setQueueCapacity(500);
        executor.setThreadNamePrefix("detect-");
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(30);
        executor.initialize();
        return executor;
    }

    @Bean(name = PARSER_EXECUTOR_POOL)
    public Executor parserExecutorPool() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(maxConcurrentParsers);
        executor.setMaxPoolSize(maxConcurrentParsers);
        executor.setQueueCapacity(200);
        executor.setThreadNamePrefix("parser-");
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(60);
        executor.initialize();
        return executor;
    }

    @Bean
    public AnalyzerBridge analyzerBridge(JobManager jobManager,
                                         @org.springframework.beans.factory.annotation.Qualifier(PARSER_EXECUTOR_POOL) Executor parserExecutorPool) {
        return new AnalyzerBridge(jobManager, parserExecutorPool, maxConcurrentParsers);
    }
}
