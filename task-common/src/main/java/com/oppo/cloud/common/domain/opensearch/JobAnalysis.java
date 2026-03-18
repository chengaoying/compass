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
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;

@Data
@Schema(name = "all abnormal job instance info will save in this index of es")
public class JobAnalysis extends OpenSearchInfo {

    @Schema(description = "users")
    private List<SimpleUser> users;

    @Schema(description = "project name")
    private String projectName;

    @Schema(description = "project Id")
    private Integer projectId;

    @Schema(description = "flow name")
    private String flowName;

    @Schema(description = "flow Id")
    private Integer flowId;

    @Schema(description = "task name")
    private String taskName;

    @Schema(description = "task Id")
    private Integer taskId;

    @Schema(description = "execution date")
    private Date executionDate;

    @Schema(description = "start time")
    private Date startTime;

    @Schema(description = "end time")
    private Date endTime;

    @Schema(description = "duration")
    private Double duration;

    @Schema(description = "task state")
    private String taskState;

    @Schema(description = "memory·seconds")
    private Double memorySeconds;

    @Schema(description = "vcore·seconds")
    private Double vcoreSeconds;

    @Schema(description = "task type")
    private String taskType;

    @Schema(description = "retry times")
    private Integer retryTimes;

    @Schema(description = "categories")
    private List<String> categories;

    @Schema(description = "baseline duration")
    private String durationBaseline;

    @Schema(description = "baseline end time")
    private String endTimeBaseline;

    @Schema(description = "last successful time(Long-term failed task)")
    private String successExecutionDay;

    @Schema(description = "days since the last success(Long-term failed task)")
    private String successDays;

    @Schema(description = "task used memory(Memory overflow warning)")
    private Double memory;

    @Schema(description = "memory usage ratio(Memory overflow warning)")
    private Double memoryRatio;

    @Schema(description = "task deletion status: not deleted (0), deleted (1)")
    private Integer deleted = 0;

    @Schema(description = "task processing status: unprocessed (0), processed (1)")
    private Integer taskStatus = 0;

    @Schema(description = "create time")
    private Date createTime;

    @Schema(description = "update time")
    private Date updateTime;

    public Map<String, Object> genDoc() throws Exception {
        Map<String, Object> doc = new HashMap<>();
        for (Field field : this.getClass().getDeclaredFields()) {
            String key = field.getName();
            String method = key.substring(0, 1).toUpperCase() + key.substring(1);
            Method getMethod = this.getClass().getMethod("get" + method);
            switch (field.getName()) {
                case "docId":
                    break;
                case "executionDate":
                case "startTime":
                case "endTime":
                case "updateTime":
                case "createTime":
                    Date value = (Date) getMethod.invoke(this);
                    if (value != null) {
                        doc.put(key, DateUtil.timestampToUTCDate(value.getTime()));
                    }
                    break;
                default:
                    doc.put(key, getMethod.invoke(this));
            }
        }
        return doc;
    }

    public String genIndex(String baseIndex) {
        return StringUtils.isNotBlank(this.getIndex()) ? this.getIndex()
                : baseIndex + "-" + DateUtil.format(this.getExecutionDate(), "yyyy-MM-dd");
    }

    public String genDocId() {
        return StringUtils.isNotBlank(this.getDocId()) ? this.getDocId() : UUID.randomUUID().toString();
    }
}
