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

package com.oppo.cloud.portal.domain.log;

import com.oppo.cloud.common.domain.opensearch.LogSummary;
import com.oppo.cloud.common.domain.opensearch.TaskApp;
import com.oppo.cloud.common.util.DateUtil;
import com.oppo.cloud.model.TaskDiagnosisAdvice;
import com.oppo.cloud.portal.util.MessageSourceUtil;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;

import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Data
@Schema(name = "Log information")
public class LogInfo {

    @Schema(description = "log type")
    private String logType;

    @Schema(description = "event description")
    private String event;

    @Schema(description = "time")
    private String logTime;

    @Schema(description = "log content")
    private String logContent;

    @Schema(description = "advice")
    private String advice;

    @Schema(description = "action")
    private String action;

    public static LogInfo genLogInfo(LogSummary logSum, TaskDiagnosisAdvice diagnoseAdvice) throws Exception {
        LogInfo logInfo = new LogInfo();
        Map<String, String> vars = logSum.getGroupData();
        logInfo.setLogType(logSum.getLogType());
        logInfo.setEvent(diagnoseAdvice.getDescription());
        if (StringUtils.isNotBlank(logSum.getAdvice())) {
            logInfo.setAdvice(logSum.getAdvice());
        } else {
            logInfo.setAdvice(diagnoseAdvice.genAdvice(vars));
        }
        logInfo.setLogTime(DateUtil.format(new Date(logSum.getLogTimestamp() * 1000L)));
        logInfo.setAction(logSum.getAction());
        logInfo.setLogContent(logSum.getRawLog());
        return logInfo;
    }

    public static LogInfo genLogInfo(TaskApp taskApp, List<TaskDiagnosisAdvice> diagnoseAdviceList) {
        LogInfo logInfo = new LogInfo();
        logInfo.setLogType("yarn");
        String content = taskApp.getDiagnostics().replaceAll("\n", "<br/>");
        logInfo.setLogContent(content);
        logInfo.setLogTime(taskApp.getStartTime() == null ? "-"
                : DateUtil.format(new Date(taskApp.getStartTime().getTime()), "yyyy-MM-dd' 'HH:mm:ss"));
        for (TaskDiagnosisAdvice diagnoseAdvice : diagnoseAdviceList) {
            String advice = diagnoseAdvice.detectDiagnostics(taskApp.getDiagnostics());
            if (advice != null) {
                logInfo.setAction(diagnoseAdvice.getAction());
                logInfo.setAdvice(advice);
                logInfo.setEvent(diagnoseAdvice.getDescription());
                break;
            }
        }
        return logInfo;
    }

    public static LinkedHashMap<String, String> getTitles() {
        LinkedHashMap<String, String> titleMap = new LinkedHashMap<>();
        titleMap.put("logType", MessageSourceUtil.get("LOG_TYPE"));
        titleMap.put("event", MessageSourceUtil.get("EVENT"));
        titleMap.put("logTime", MessageSourceUtil.get("LOG_TIME"));
        titleMap.put("logContent", MessageSourceUtil.get("LOG_CONTENT"));
        titleMap.put("advice", MessageSourceUtil.get("ADVICE"));
        return titleMap;
    }

}
