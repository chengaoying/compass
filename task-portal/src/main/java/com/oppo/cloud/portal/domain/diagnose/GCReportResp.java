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

import com.oppo.cloud.common.domain.gc.GCReport;
import com.oppo.cloud.common.domain.gc.HeapUsed;
import com.oppo.cloud.common.domain.gc.TenuredUsed;
import com.oppo.cloud.common.domain.gc.YoungUsed;
import com.oppo.cloud.portal.util.UnitUtil;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;
import org.springframework.beans.BeanUtils;

import java.util.List;

@Data
public class GCReportResp {

    @Schema(description = "Maximum allocated memory")
    private String maxHeapAllocatedSize;

    @Schema(description = "Maximum memory usage")
    private String maxHeapUsedSize;

    @Schema(description = "Total time")
    private String totalTime;

    @Schema(description = "YG count/time (s)")
    private String YGCountAndDuration;

    @Schema(description = "FG count/time (s)")
    private String FGCountAndDuration;

    @Schema(description = "GC count/time (s)")
    private String GCCountAndDuration;

    @Schema(description = "Heap usage trend chart")
    private List<HeapUsed> heapUsed;

    @Schema(description = "Tenured usage trend chart")
    private List<TenuredUsed> tenuredUsed;

    @Schema(description = "Young usage trend chart")
    private List<YoungUsed> youngUsed;

    public void build(GCReport gcReport) {
        this.totalTime = gcReport.getTotalTime();
        this.heapUsed = gcReport.getHeapUsed();
        this.tenuredUsed = gcReport.getTenuredUsed();
        this.youngUsed = gcReport.getYoungUsed();
        this.maxHeapAllocatedSize =
                String.format("%.2fGB", UnitUtil.transferKBToGB((long) gcReport.getMaxHeapAllocatedSize()));
        this.maxHeapUsedSize = String.format("%.2fGB", UnitUtil.transferKBToGB((long) gcReport.getMaxHeapUsedSize()));
        this.YGCountAndDuration = String.format("%d/%.2f", gcReport.getYoungGCCount(), gcReport.getYoungGCTime());
        this.FGCountAndDuration = String.format("%d/%.2f", gcReport.getFullGCCount(), gcReport.getFullGCTime());
        this.GCCountAndDuration = String.format("%d/%.2f", gcReport.getTotalGCCount(), gcReport.getTotalGCTime());
    }
}
