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

package com.oppo.cloud.portal.domain.diagnose.resources;

import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;

@Data
@Schema(name = "SparkConfig")
public class SparkConfig {

    @Schema(description = "driver memory")
    private double sparkDriverMemory;

    @Schema(description = "executor memory")
    private double sparkExecutorMemory;

    public SparkConfig(double sparkDriverMemory, double sparkExecutorMemory) {
        this.sparkDriverMemory = sparkDriverMemory;
        this.sparkExecutorMemory = sparkExecutorMemory;
    }
}
