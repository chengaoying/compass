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

package com.oppo.cloud.portal.domain.diagnose;

import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Data
@Schema(name = "Chart Type")
public class Chart<T> {

    @Schema(description = "X-axis")
    private String x;

    @Schema(description = "Y-axis")
    private String y;

    @Schema(description = "Y-axis unit")
    private String unit;

    @Schema(description = "Chart data")
    private List<T> dataList = new ArrayList<>();

    @Schema(description = "Data category description (different data displayed in different colors)")
    private Map<String, ChartInfo> dataCategory;

    @Schema(description = "Chart description")
    private String des;

    @Data
    public static class ChartInfo {

        private String name;
        private String color;

        public ChartInfo(String name, String color) {
            this.color = color;
            this.name = name;
        }
    }
}
