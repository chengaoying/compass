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

package com.oppo.cloud.analyzer;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * task-analyzer: merged from task-detect + task-parser.
 *
 * <p>Eliminates the Redis queue intermediary between detection and parsing by directly
 * calling the parser (JobManager) after detection completes. Data flow:
 *
 * <pre>
 *   Kafka[task-instance]
 *       → DetectedTask (Kafka consumer, batch=50)
 *       → DetectService.detect() [6 workflow detectors]
 *       → AnalyzerBridge.analyze(logRecord)  ← NEW: replaces Redis push
 *       → JobManager.run(logRecord)           ← was in separate process
 *       → Kafka[task-gpt]
 * </pre>
 */
@SpringBootApplication(scanBasePackages = {
        "com.oppo.cloud.analyzer",
        "com.oppo.cloud.detect",
        "com.oppo.cloud.parser",
        "com.oppo.cloud.common"
})
@MapperScan({"com.oppo.cloud.mapper", "com.oppo.cloud.detect.mapper", "com.oppo.cloud.parser.mapper"})
@EnableScheduling
@EnableAsync
public class TaskAnalyzerApplication {

    public static void main(String[] args) {
        SpringApplication.run(TaskAnalyzerApplication.class, args);
    }
}
