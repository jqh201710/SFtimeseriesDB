package com.timeseries.db.web.dto;

import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * 多指标查询响应DTO
 */
@Data
public class MultiMetricQueryResponse {
    /** 按指标名分组的结果：key=measurement，value=该指标的时序数据 */
    private Map<String, List<SimpleQueryResponse.SimpleDataPoint>> metricData;
}