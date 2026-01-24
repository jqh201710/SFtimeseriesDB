package com.timeseries.db.controller;

import com.timeseries.db.core.DataPoint;

import java.util.List;

class BatchWriteRequest {
    private String series;
    private List<DataPoint> points;

    // Getters and Setters
    public String getSeries() { return series; }
    public void setSeries(String series) { this.series = series; }

    public List<DataPoint> getPoints() { return points; }
    public void setPoints(List<DataPoint> points) { this.points = points; }
}
