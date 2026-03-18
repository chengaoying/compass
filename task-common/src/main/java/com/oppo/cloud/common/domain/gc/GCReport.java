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

package com.oppo.cloud.common.domain.gc;

import com.oppo.cloud.common.util.DateUtil;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
@Schema(name = "GC log analysis.")
public class GCReport {

    /**
     * Executor ID corresponding to the GC log.
     */
    @Schema(description = "Executor ID corresponding to the GC log.")
    private Integer executorId;
    /**
     * Maximum allocated memory.
     */
    @Schema(description = "Maximum allocated memory.")
    private Integer maxHeapAllocatedSize;
    /**
     * Maximum memory usage.
     */
    @Schema(description = "Maximum memory usage.")
    private Integer maxHeapUsedSize;
    /**
     * Running time.
     */
    @Schema(description = "Running time.")
    private String totalTime;
    /**
     * Number of young GCs.
     */
    @Schema(description = "Number of young GCs.")
    private Integer youngGCCount;
    /**
     * Young GC time.
     */
    @Schema(description = "Young GC time.")
    private Double youngGCTime;
    /**
     * Number of full GCs.
     */
    @Schema(description = "Number of full GCs.")
    private Integer fullGCCount;
    /**
     * Full GC time.
     */
    @Schema(description = "Full GC time.")
    private Double fullGCTime;
    /**
     * Total number of GCs.
     */
    @Schema(description = "Total number of GCs.")
    private Integer totalGCCount;
    /**
     * Total GC time.
     */
    @Schema(description = "Total GC time.")
    private Double totalGCTime;
    /**
     * Heap usage trend chart.
     */
    @Schema(description = "Heap usage trend chart.")
    private List<HeapUsed> heapUsed;
    /**
     * Tenured usage trend chart.
     */
    @Schema(description = "Tenured usage trend chart.")
    private List<TenuredUsed> tenuredUsed;
    /**
     * Young usage trend chart.
     */
    @Schema(description = "Young usage trend chart.")
    private List<YoungUsed> youngUsed;
    /**
     * appId
     */
    @Schema(description = "appId")
    private String applicationId;
    /**
     * Log type: driver executor.
     */
    @Schema(description = "Log type: driver executor.")
    private String logType;
    /**
     * Log path in hdfs
     */
    @Schema(description = "Log path in hdfs")
    private String logPath;
    /**
     * Executor's memory usage.
     */
    @Schema(description = "Executor's memory usage.")
    private List<ExecutorPeakMemory> executorPeakMemory;

    public Map<String, Object> genDoc() throws Exception {
        Map<String, Object> res = new HashMap<>();
        Field[] fileds = this.getClass().getDeclaredFields();
        for (Field field : fileds) {
            String key = field.getName();
            String method = key.substring(0, 1).toUpperCase() + key.substring(1);
            Method getMethod = this.getClass().getMethod("get" + method);
            res.put(key, getMethod.invoke(this));
        }
        return res;
    }

}
