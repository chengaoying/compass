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

package com.oppo.cloud.portal.domain.diagnose.oneclick;

import com.oppo.cloud.portal.domain.task.TaskAppInfo;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
@Schema(name = "result of one-click diagnosis")
public class DiagnoseResult {

    @Schema(description = "diagnosing status(failed, success, processing)")
    private String status;

    @Schema(description = "processing information")
    private List<ProcessInfo> processInfoList = new ArrayList<>();

    @Schema(description = "task information")
    private TaskAppInfo taskAppInfo;

    @Schema(description = "exception")
    private String errorMsg;

    @Data
    @Schema(name = "processing information")
    public static class ProcessInfo {

        @Schema(description = "message")
        private String msg;
        @Schema(description = "speed")
        private double speed;
        public ProcessInfo(String msg, double speed) {
            this.msg = msg;
            this.speed = speed;
        }
    }
}
