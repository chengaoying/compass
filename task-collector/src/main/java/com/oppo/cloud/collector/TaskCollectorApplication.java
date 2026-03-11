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

package com.oppo.cloud.collector;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * task-collector: merged from task-application + task-metadata.
 *
 * <p>Unifies two services that both deal with "discovering YARN/Spark application IDs
 * and their log paths":
 *
 * <ul>
 *   <li><b>task-application</b> (event-driven): Kafka[task-instance] consumer →
 *       reads HDFS scheduler logs → extracts applicationId → publishes Kafka[task-application]</li>
 *   <li><b>task-metadata</b> (scheduled polling): Spark HistoryServer + YARN RM REST APIs →
 *       writes app metadata to OpenSearch</li>
 * </ul>
 *
 * <p>Both scan base packages are included so existing Spring components need no code changes.
 * The two entry-point modes (Kafka listener + @Scheduled jobs) run concurrently in the same JVM.
 */
@SpringBootApplication(scanBasePackages = {
        "com.oppo.cloud.collector",
        "com.oppo.cloud.application",
        "com.oppo.cloud.meta",
        "com.oppo.cloud.common"
})
@MapperScan({"com.oppo.cloud.mapper", "com.oppo.cloud.application.dao"})
@EnableScheduling
@EnableAsync
public class TaskCollectorApplication {

    public static void main(String[] args) {
        SpringApplication.run(TaskCollectorApplication.class, args);
    }
}
