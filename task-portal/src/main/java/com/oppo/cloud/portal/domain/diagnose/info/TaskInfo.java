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

import java.util.List;

@Data
@Schema(name = "information of task")
public class TaskInfo {

    @Schema(description = "task name")
    private String taskName;

    @Schema(description = "flow name")
    private String flowName;

    @Schema(description = "project name")
    private String projectName;

    @Schema(description = "execution time")
    private String executionTime;

    @Schema(description = "running duration of application")
    private String appTime;

    @Schema(description = "applicationId")
    private String applicationId;

    @Schema(description = "categories of exception")
    private List<String> categories;

    @Schema(description = "memory consuming")
    private String memorySeconds;

    @Schema(description = "cpu consuming")
    private String vcoreSeconds;
}
