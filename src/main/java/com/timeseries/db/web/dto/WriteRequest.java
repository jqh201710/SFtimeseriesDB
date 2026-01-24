package com.timeseries.db.web.dto;

import lombok.Data;

import java.util.Map;

@Data
public class WriteRequest {
    private String measurement;
    private Map<String, String> tags;
    private Map<String, Object> fields;
    private long timestamp; // 毫秒级时间戳
}
