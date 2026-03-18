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
import com.oppo.cloud.portal.domain.report.ReportRequest;
import com.oppo.cloud.portal.service.ReportService;
import io.swagger.v3.oas.annotations.tags.Tag;
import io.swagger.v3.oas.annotations.Parameter;

import io.swagger.v3.oas.annotations.Operation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@Slf4j
@RestController
@RequestMapping(value = "/api/v1/report")
@Tag(name = "ReportController", description = "API for general report")
public class ReportController {

    @Autowired
    private ReportService reportService;

    @GetMapping(value = "/statistics")
    @Operation(summary = "get offline statistic of general report ")
    public CommonStatus<?> getStatisticData(@Parameter(description = "project name") @RequestParam(value = "projectName", required = false) String projectName) throws Exception {
        return CommonStatus.success(reportService.getStatisticsData(projectName));
    }

    @PostMapping(value = "/graph")
    @Operation(summary = "report overview chart")
    public CommonStatus<?> getGraph(@RequestBody ReportRequest reportRequest) throws Exception {
        return CommonStatus.success(reportService.getGraph(reportRequest));
    }

    @Operation(summary = "list of project")
    @RequestMapping(value = "/projects", method = RequestMethod.GET)
    @ResponseBody
    public CommonStatus<?> getProjects() throws Exception {
        return CommonStatus.success(reportService.getProjects());
    }

}
