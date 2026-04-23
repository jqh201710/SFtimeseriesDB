package com.timeseries.db.core.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Map;

/**
 * 时序数据点（对标InfluxDB的Point）
 * measurement: 测量值（如cpu_usage）
 * tags: 标签（维度，如host=192.168.1.1）
 * fields: 字段（数值，如value=0.85）
 * timestamp: 时间戳（毫秒）
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Point implements Serializable {
    @JsonProperty("m")
    private String measurement;
    @JsonProperty("t")
    private Map<String, String> tags;
    @JsonProperty("f")
    private Map<String, Object> fields;
    @JsonProperty("ts")
    private long timestamp;
}
