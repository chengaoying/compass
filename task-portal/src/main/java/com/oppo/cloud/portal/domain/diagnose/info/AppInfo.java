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

package com.oppo.cloud.portal.domain.diagnose.info;

import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;

@Data
@Schema(name = "Parameters of Spark application")
public class AppInfo {

    @Schema(description = "spark.driver.memoryOverhead")
    private String driverOverhead;

    @Schema(description = "spark.driver.memory")
    private String driverMemory;

    @Schema(description = "spark.executor.memoryOverhead")
    private String executorOverhead;

    @Schema(description = "spark.executor.memory")
    private String executorMemory;

    @Schema(description = "spark.dynamicAllocation.maxExecutors")
    private String maxExecutors;

    @Schema(description = "spark.executor.cores")
    private String executorCores;

    @Schema(description = "spark.default.parallelism")
    private String parallelism;

    @Schema(description = "spark.sql.shuffle.partitions")
    private String shufflePartitions;
}
