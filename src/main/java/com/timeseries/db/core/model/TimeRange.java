// com.timeseries.db.core.model/TimeRange.java
package com.timeseries.db.core.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TimeRange {
    private long start; // 开始时间戳（毫秒）
    private long end;   // 结束时间戳（毫秒）
}