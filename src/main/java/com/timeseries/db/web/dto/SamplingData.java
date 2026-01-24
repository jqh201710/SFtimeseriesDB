package com.timeseries.db.web.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 采样数据项：时间戳+对应值
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SamplingData {
    /** 采样时间戳（毫秒） */
    private long timestamp;
    /** 该采样点的字段值 */
    private Object value;
}