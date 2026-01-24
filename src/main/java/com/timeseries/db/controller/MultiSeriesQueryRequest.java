package com.timeseries.db.controller;

import java.util.List;
import java.util.Map;

class MultiSeriesQueryRequest {
    private List<String> seriesList;
    private long startTime;
    private long endTime;
    private Map<String, String> tags;

    // Getters and Setters
    public List<String> getSeriesList() { return seriesList; }
    public void setSeriesList(List<String> seriesList) { this.seriesList = seriesList; }

    public long getStartTime() { return startTime; }
    public void setStartTime(long startTime) { this.startTime = startTime; }

    public long getEndTime() { return endTime; }
    public void setEndTime(long endTime) { this.endTime = endTime; }

    public Map<String, String> getTags() { return tags; }
    public void setTags(Map<String, String> tags) { this.tags = tags; }
}
