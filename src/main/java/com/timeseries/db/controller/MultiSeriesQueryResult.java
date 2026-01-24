package com.timeseries.db.controller;

import com.timeseries.db.core.DataPoint;

import java.util.List;
import java.util.Map;

class MultiSeriesQueryResult {
    private List<String> seriesList;
    private long startTime;
    private long endTime;
    private int pointCount;
    private List<DataPoint> points;
    private Map<String, String> tags;

    // Getters and Setters
    public List<String> getSeriesList() { return seriesList; }
    public void setSeriesList(List<String> seriesList) { this.seriesList = seriesList; }

    public long getStartTime() { return startTime; }
    public void setStartTime(long startTime) { this.startTime = startTime; }

    public long getEndTime() { return endTime; }
    public void setEndTime(long endTime) { this.endTime = endTime; }

    public int getPointCount() { return pointCount; }
    public void setPointCount(int pointCount) { this.pointCount = pointCount; }

    public List<DataPoint> getPoints() { return points; }
    public void setPoints(List<DataPoint> points) { this.points = points; }

    public Map<String, String> getTags() { return tags; }
    public void setTags(Map<String, String> tags) { this.tags = tags; }
}
