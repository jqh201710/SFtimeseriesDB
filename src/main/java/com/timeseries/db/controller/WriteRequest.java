package com.timeseries.db.controller;

import java.util.Map;

// ========== 请求/响应对象 ==========

class WriteRequest {
    private String series;
    private long timestamp;
    private double value;
    private Map<String, String> tags;

    // Getters and Setters
    public String getSeries() { return series; }
    public void setSeries(String series) { this.series = series; }

    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }

    public double getValue() { return value; }
    public void setValue(double value) { this.value = value; }

    public Map<String, String> getTags() { return tags; }
    public void setTags(Map<String, String> tags) { this.tags = tags; }
}
