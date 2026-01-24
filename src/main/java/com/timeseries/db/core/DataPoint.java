package com.timeseries.db.core;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class DataPoint {
    private final long timestamp;
    private final double value;
    private final Map<String, String> tags;

    public DataPoint(long timestamp, double value, Map<String, String> tags) {
        this.timestamp = timestamp;
        this.value = value;
        this.tags = tags != null ? new HashMap<>(tags) : new HashMap<>();
    }

    public long getTimestamp() { return timestamp; }
    public double getValue() { return value; }
    public Map<String, String> getTags() {
        return Collections.unmodifiableMap(tags);
    }
}
