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

package com.oppo.cloud.portal.domain.blocklist;

import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

@Data
@Schema(name = "Query blocklist request")
public class BlocklistReq {

    @Schema(description = "component: flink or spark")
    private String component;
    @Schema(description = "project name")
    private String projectName;

    @Schema(description = "flow name")
    private String flowName;

    @Schema(description = "task name")
    private String taskName;

    @Schema(description = "page number")
    @Min(value = 1, message = "page page is more than 0")
    @NotNull(message = "page is not empty")
    private Integer page = 1;

    @Schema(description = "page size")
    @NotNull(message = "pageSize is not empty")
    @Max(value = 500, message = "pageSize less than 500")
    private Integer pageSize = 10;

}
