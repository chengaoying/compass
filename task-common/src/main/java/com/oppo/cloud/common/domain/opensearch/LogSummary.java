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

package com.oppo.cloud.common.domain.opensearch;

import com.oppo.cloud.common.util.DateUtil;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;


@Data
public class LogSummary extends OpenSearchInfo {

    @Schema(description = "applicationId")
    private String applicationId;

    @Schema(description = "log type")
    private String logType;

    @Schema(description = "project name")
    private String projectName;

    @Schema(description = "flow name")
    private String flowName;

    @Schema(description = "task name")
    private String taskName;

    @Schema(description = "execution date")
    private Date executionDate;

    @Schema(description = "retry times")
    private Integer retryTimes;

    @Schema(description = "action type")
    private String action;

    @Schema(description = "step")
    private Integer step;

    @Schema(description = "group names")
    private List<String> groupNames;

    @Schema(description = "raw log")
    private String rawLog;

    @Schema(description = "log path")
    private String logPath;

    @Schema(description = "group data")
    private Map<String, String> groupData;

    @Schema(description = "log timestamp")
    private Integer logTimestamp;

    @Schema(description = "advice")
    private String advice;

    public Map<String, Object> genDoc() throws Exception {
        Map<String, Object> res = new HashMap<>();
        Field[] fileds = this.getClass().getDeclaredFields();
        for (Field field : fileds) {
            String key = field.getName();
            String method = key.substring(0, 1).toUpperCase() + key.substring(1);
            Method getMethod = this.getClass().getMethod("get" + method);
            switch (field.getName()) {
                case "executionDate":
                    Date value = (Date) getMethod.invoke(this);
                    if (value != null) {
                        res.put(key, DateUtil.timestampToUTCDate(value.getTime()));
                    }
                    break;
                default:
                    res.put(key, getMethod.invoke(this));
            }
        }
        res.put("docId", UUID.randomUUID().toString());
        return res;
    }
}
