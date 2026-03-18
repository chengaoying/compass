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

package com.oppo.cloud.portal.domain.diagnose;

import com.oppo.cloud.portal.domain.diagnose.info.AppInfo;
import com.oppo.cloud.portal.domain.diagnose.info.ClusterInfo;
import com.oppo.cloud.portal.domain.diagnose.info.TaskInfo;
import com.oppo.cloud.portal.domain.diagnose.runerror.RunError;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Data
@Schema(name = "DiagnoseReport")
public class DiagnoseReport {

    @Schema(description = "run information")
    private RunInfo runInfo;

    @Schema(description = "run error")
    private List<Item<RunError>> runErrorAnalyze = new ArrayList<>();

    @Schema(description = "resources")
    private List<Item> resourcesAnalyze = new ArrayList<>();

    @Schema(description = "run time")
    private List<Item> runTimeAnalyze = new ArrayList<>();

    @Data
    public static class RunInfo {

        @Schema(description = "task information")
        private TaskInfo taskInfo;

        @Schema(description = "cluster information")
        private ClusterInfo clusterInfo;

        @Schema(description = "app parameter")
        private Map<String, Object> env;

        @Schema(description = "error information")
        private String error;
    }

}
