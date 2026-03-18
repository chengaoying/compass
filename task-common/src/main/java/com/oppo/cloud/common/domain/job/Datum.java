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

package com.oppo.cloud.common.domain.job;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;

import java.util.List;
import java.util.Set;


@Data
public class Datum {

    @Schema(description = "Node")
    Set<Node> nodeList;

    @Schema(description = "Edge")
    List<Verge> vergeList;

    @Data
    public static class Node {

        @Schema(description = "Task id")
        private Integer id;
        @Schema(description = "Task name")
        private String taskName;
        @Schema(description = "Flow name")
        private String flowName;
        @Schema(description = "Project name")
        private String projectName;
        @Schema(description = "Task start time")
        private String startTime;
        @Schema(description = "Task end time")
        private String endTime;
        @Schema(description = "Execution Date")
        private String executionDate;
        @Schema(description = "Task execution time consumption")
        private String duration;
        @Schema(description = "Baseline for execution time consumption")
        private String durationBaseLine;
        @Schema(description = "Baseline for completion time")
        private String endTimeBaseLine;
        @Schema(description = "Whether the completion time is abnormal")
        private Boolean endTimeAbnormal = false;
        @Schema(description = "Whether the execution time is abnormal")
        private Boolean durationAbnormal = false;
        @Schema(description = "Time period of task execution")
        private String period;
        @Schema(description = "Task state")
        private String taskState;

        // Duplicate
        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Node other = (Node) obj;
            return this.id.equals(other.id);
        }

        @Override
        public int hashCode() {
            return this.id.hashCode();
        }
    }

    @Data
    public static class Verge {

        @Schema(description = "Upstream task")
        private Integer upstream;

        @Schema(description = "Downstream task")
        private Integer downStream;

        public Verge(Integer upstream, Integer downStream) {
            this.upstream = upstream;
            this.downStream = downStream;
        }
    }
}
