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

package com.oppo.cloud.portal.domain.diagnose.runtime;

import com.oppo.cloud.portal.domain.diagnose.IsAbnormal;
import com.oppo.cloud.portal.domain.diagnose.Table;
import com.oppo.cloud.portal.util.MessageSourceUtil;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;
import org.opensearch.common.inject.spi.Message;

import java.util.LinkedHashMap;


@Data
public class GlobalSort extends IsAbnormal {

    @Schema(description = "table information")
    private Table<GlobalSortTable> table = new Table<>();

    public GlobalSort() {
        LinkedHashMap<String, String> titleMap = new LinkedHashMap<>();
        titleMap.put("jobId", "JobId");
        titleMap.put("stageId", "StageId");
        titleMap.put("taskNum", MessageSourceUtil.get("TASK_NUM"));
        titleMap.put("dataOfColumns", MessageSourceUtil.get("DATA_OF_COLUMN"));
        titleMap.put("duration", MessageSourceUtil.get("DURATION"));
        table.setTitles(titleMap);
    }

    @Data
    public static class GlobalSortTable {

        @Schema(description = "JobId")
        private String jobId;

        @Schema(description = "stageId")
        private String stageId;

        @Schema(description = "task number")
        private String taskNum;

        @Schema(description = "task size")
        private String dataOfColumns;

        @Schema(description = "duration")
        private String duration;
    }
}
