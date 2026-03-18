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

package com.oppo.cloud.portal.controller;

import com.oppo.cloud.common.api.CommonStatus;
import com.oppo.cloud.common.constant.AppCategoryEnum;
import com.oppo.cloud.portal.domain.diagnose.GCReportResp;
import com.oppo.cloud.portal.domain.diagnose.oneclick.DiagnoseResult;
import com.oppo.cloud.portal.domain.task.JobsRequest;
import com.oppo.cloud.portal.domain.task.TaskAppsRequest;
import com.oppo.cloud.portal.service.OneClickDiagnosisService;
import com.oppo.cloud.portal.service.TaskAppService;
import io.swagger.v3.oas.annotations.tags.Tag;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.Operation;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

/**
 * application interface
 */
@RestController
@RequestMapping("/api/v1/app")
@Tag(name = "AppController", description = "app interface")
@Slf4j
public class AppController {

    @Autowired
    private TaskAppService taskAppService;

    @Autowired
    private OneClickDiagnosisService oneClickDiagnosisService;

    @PostMapping(value = "/list")
    @Operation(summary = "application list")
    @ResponseBody
    public CommonStatus<?> searchApplications(@Validated @RequestBody TaskAppsRequest request) throws Exception {
        return CommonStatus.success(taskAppService.searchTaskApps(request));
    }

    @GetMapping(value = "/report")
    @Operation(summary = "diagnose report")
    public CommonStatus<?> getDiagnoseReport(@Parameter(description = "applicationId") @RequestParam(value = "applicationId") String applicationId) throws Exception {
        return CommonStatus.success(taskAppService.generateReport(applicationId));
    }

    @GetMapping(value = "/report/runError")
    @Operation(summary = "diagnose runError of report")
    public CommonStatus<?> getDiagnoseReportRunError(@Parameter(description = "applicationId") @RequestParam(value = "applicationId") String applicationId) throws Exception {
        return CommonStatus.success(taskAppService.diagnoseRunError(applicationId));
    }

    @GetMapping(value = "/report/runInfo")
    @Operation(summary = "diagnose runInfo of report")
    public CommonStatus<?> getDiagnoseReportRunInfo(@Parameter(description = "applicationId") @RequestParam(value = "applicationId") String applicationId) throws Exception {
        return CommonStatus.success(taskAppService.diagnoseRunInfo(applicationId));
    }

    @GetMapping(value = "/report/runResource")
    @Operation(summary = "diagnose runResource of report")
    public CommonStatus<?> getDiagnoseReportRunResource(@Parameter(description = "applicationId") @RequestParam(value = "applicationId") String applicationId) throws Exception {
        return CommonStatus.success(taskAppService.diagnoseRunResource(applicationId));
    }

    @GetMapping(value = "/report/runTime")
    @Operation(summary = "diagnose runTime of report")
    public CommonStatus<?> getDiagnoseReportRunTime(@Parameter(description = "applicationId") @RequestParam(value = "applicationId") String applicationId) throws Exception {
        return CommonStatus.success(taskAppService.diagnoseRunTime(applicationId));
    }

    @GetMapping(value = "/categories")
    @Operation(summary = "app category type")
    public CommonStatus<?> getCategories() {
        return CommonStatus.success(AppCategoryEnum.getAllLangMsg());
    }

    @PostMapping(value = "/graph")
    @Operation(summary = "task graph")
    public CommonStatus<?> getGraph(@Validated @RequestBody JobsRequest request) throws Exception {
        return CommonStatus.success(taskAppService.getGraph(request));
    }

    @GetMapping(value = "/diagnose")
    @Operation(summary = "one-click diagnosis")
    public CommonStatus<DiagnoseResult> getAppDiagnose(@Parameter(description = "applicationId") @RequestParam(value = "applicationId") String applicationId) throws Exception {
        if (StringUtils.isNotEmpty(applicationId)) {
            return CommonStatus.success(oneClickDiagnosisService.diagnose(applicationId));
        } else {
            return CommonStatus.failed(String.format("Invalid applicationId: %s", applicationId));
        }
    }

    @GetMapping(value = "/gc")
    @Operation(summary = "GC log analysis")
    public CommonStatus<GCReportResp> getGCReport(@Parameter(description = "applicationId") @RequestParam(value = "applicationId") String applicationId,
                                                  @Parameter(description = "executor") @RequestParam(value = "executorId") String executorId) throws Exception {
        return CommonStatus.success(taskAppService.getGcReport(applicationId, executorId));
    }
}
