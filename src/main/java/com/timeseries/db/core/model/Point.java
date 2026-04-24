package com.timeseries.db.core.model;

import com.fasterxml.jackson.annotation.JsonAlias;
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
 *
 * 序列化使用短键名节省空间（方案A），同时通过 @JsonAlias 兼容旧数据的长键名
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Point implements Serializable {
    @JsonProperty("m")
    @JsonAlias("measurement")
    private String measurement;

    @JsonProperty("t")
    @JsonAlias("tags")
    private Map<String, String> tags;

    @JsonProperty("f")
    @JsonAlias("fields")
    private Map<String, Object> fields;

    @JsonProperty("ts")
    @JsonAlias("timestamp")
    private long timestamp;
}
