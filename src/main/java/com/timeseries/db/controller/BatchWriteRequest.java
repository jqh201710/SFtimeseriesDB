package com.timeseries.db.controller;

import com.timeseries.db.core.DataPoint;

import java.util.List;


// 修复 BatchWriteRequest 类
class BatchWriteRequest {
    private String series;
    private List<DataPoint> points;

    // 必须有无参构造函数
    public BatchWriteRequest() {
    }

    public BatchWriteRequest(String series, List<DataPoint> points) {
        this.series = series;
        this.points = points;
    }

    // Getters and Setters
    public String getSeries() { return series; }
    public void setSeries(String series) { this.series = series; }

    public List<DataPoint> getPoints() { return points; }
    public void setPoints(List<DataPoint> points) { this.points = points; }
}
