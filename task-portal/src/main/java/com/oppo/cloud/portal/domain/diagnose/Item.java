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

import com.oppo.cloud.portal.domain.base.Conclusion;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;

@Data
@Schema(name = "Item")
public class Item<T> {

    @Schema(description = "name")
    private String name;

    @Schema(description = "conclusion")
    private Conclusion conclusion;

    @Schema(description = "item content")
    private T item;

    @Schema(description = "error")
    private String error;

    @Schema(description = "type")
    private String type;
}
