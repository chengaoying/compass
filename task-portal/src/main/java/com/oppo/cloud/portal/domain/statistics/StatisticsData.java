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

package com.oppo.cloud.portal.domain.statistics;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;

@Data
public class StatisticsData {

    @Schema(description = "Number of abnormal jobs")
    private Integer abnormalJobNum;

    @Schema(description = "Number of active jobs")
    private Integer jobNum;

    @Schema(description = "Ratio of abnormal jobs")
    private Double abnormalJobNumRatio;

    @Schema(description = "Chain ratio of abnormal jobs")
    private Double abnormalJobNumChainRatio;

    @Schema(description = "Day On Day ratio of abnormal jobs")
    private Double abnormalJobNumDayOnDay;

    @Schema(description = "Number of abnormal instances")
    private Integer abnormalJobInstanceNum;

    @Schema(description = "Number of job instances")
    private Integer jobInstanceNum;

    @Schema(description = "Ratio of abnormal instances")
    private Double abnormalJobInstanceNumRatio;

    @Schema(description = "Chain ratio of abnormal instances")
    private Double abnormalJobInstanceNumChainRatio;

    @Schema(description = "Day On Day ratio of abnormal instances")
    private Double abnormalJobInstanceNumDayOnDay;

    @Schema(description = "Abnormal Job CPU number")
    private Double abnormalJobCpuNum;

    @Schema(description = "Job CPU number")
    private Double jobCpuNum;

    @Schema(description = "CPU unit")
    private String cpuUnit = "vcore·s";

    @Schema(description = "Ratio of job CPU number")
    private Double abnormalJobCpuNumRatio;

    @Schema(description = "Chain ratio of job CPU number")
    private Double abnormalJobCpuNumChainRatio;

    @Schema(description = "Day On Day ratio of job CPU number")
    private Double abnormalJobCpuNumDayOnDay;

    @Schema(description = "Abnormal job memory number")
    private Double abnormalJobMemoryNum;

    @Schema(description = "Job memory number")
    private Double jobMemoryNum;

    @Schema(description = "Memory unit")
    private String memoryUnit = "G·s";

    @Schema(description = "Ratio of job memory number")
    private Double abnormalJobMemoryNumRatio;

    @Schema(description = "Chain ratio of job memory number")
    private Double abnormalJobMemoryNumChainRatio;

    @Schema(description = "Day On Day ratio of job memory number")
    private Double abnormalJobMemoryNumDayOnDay;
}
