// core/DataPoint.java - 使用 Lombok 简化
package com.timeseries.db.core;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Data
@NoArgsConstructor
public class DataPoint {
    private long timestamp;
    private double value;
    private Map<String, String> tags;

    @JsonCreator
    public DataPoint(
            @JsonProperty("timestamp") long timestamp,
            @JsonProperty("value") double value,
            @JsonProperty("tags") Map<String, String> tags) {
        this.timestamp = timestamp;
        this.value = value;
        this.tags = tags != null ? new HashMap<>(tags) : new HashMap<>();
    }

    public Map<String, String> getTags() {
        return Collections.unmodifiableMap(tags);
    }

    public void setTags(Map<String, String> tags) {
        this.tags = tags != null ? new HashMap<>(tags) : new HashMap<>();
    }
}