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

package com.oppo.cloud.portal.domain.task;

import com.oppo.cloud.common.constant.AppCategoryEnum;
import com.oppo.cloud.common.domain.opensearch.SimpleUser;
import com.oppo.cloud.common.domain.opensearch.TaskApp;
import com.oppo.cloud.common.util.DateUtil;
import com.oppo.cloud.portal.util.TaskUtil;
import com.oppo.cloud.portal.util.UnitUtil;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.stream.Collectors;

@Data
@Schema(name = "Application information")
@NoArgsConstructor
public class TaskAppInfo {

    @Schema(description = "appId")
    private String applicationId;

    @Schema(description = "application type")
    private String applicationType;

    @Schema(description = "project name")
    private String projectName;

    @Schema(description = "flow name")
    private String flowName;

    @Schema(description = "task name")
    private String taskName;

    @Schema(description = "execution date")
    private String executionDate;

    @Schema(description = "categories")
    private List<String> categories;

    @Schema(description = "duration")
    private String duration;

    @Schema(description = "try number")
    private Integer tryNumber;

    @Schema(description = "resource")
    private String resource;

    @Schema(description = "users")
    private String users;

    @Schema(description = "sparkUI")
    private String sparkUI;

    @Schema(description = "task app state")
    private String taskAppState;

    /**
     * format TaskApp
     */
    public static TaskAppInfo from(TaskApp taskApp) {
        TaskAppInfo taskAppInfo = new TaskAppInfo();
        taskAppInfo.setApplicationId(taskApp.getApplicationId());
        taskAppInfo.setApplicationType(taskApp.getApplicationType());
        taskAppInfo.setProjectName(taskApp.getProjectName());
        taskAppInfo.setFlowName(taskApp.getFlowName());
        taskAppInfo.setTaskName(taskApp.getTaskName());
        taskAppInfo.setExecutionDate(DateUtil.format(taskApp.getExecutionDate()));
        taskAppInfo.setDuration(UnitUtil.transferSecond(taskApp.getElapsedTime() / 1000));
        taskAppInfo.setCategories(AppCategoryEnum.getLangMsgByCategories(taskApp.getCategories()));
        taskAppInfo.setUsers(taskApp.getUsers() == null ? ""
                : taskApp.getUsers().stream().map(SimpleUser::getUsername).collect(Collectors.joining(",")));
        taskAppInfo.setResource(TaskUtil.resourceSimplify(taskApp.getVcoreSeconds(), taskApp.getMemorySeconds()));
        taskAppInfo.setSparkUI(taskApp.getSparkUI());
        taskAppInfo.setTryNumber(taskApp.getRetryTimes() == null ? 0 : taskApp.getRetryTimes());
        taskAppInfo.setTaskAppState(taskApp.getTaskAppState());
        return taskAppInfo;
    }
}
