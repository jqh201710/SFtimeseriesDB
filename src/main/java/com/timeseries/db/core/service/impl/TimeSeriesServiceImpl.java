package com.timeseries.db.core.service.impl;

import com.timeseries.db.core.model.Point;
import com.timeseries.db.core.model.Query;
import com.timeseries.db.core.model.TimeRange;
import com.timeseries.db.core.storage.FileStorageEngine;
import com.timeseries.db.core.service.TimeSeriesService;
import com.timeseries.db.web.dto.SamplingData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

@Service
public class TimeSeriesServiceImpl implements TimeSeriesService {

    @Autowired
    private FileStorageEngine fileStorageEngine;

    // 原有方法保留
    @Override
    public void write(Point point) {
        fileStorageEngine.write(point);
    }

    @Override
    public void batchWrite(List<Point> points) {
        for (Point point : points) {
            fileStorageEngine.write(point);
        }
    }

    @Override
    public List<Point> query(Query query) {
        return fileStorageEngine.query(query);
    }

    // ========== 新增扩展方法实现 ==========
    @Override
    public List<SamplingData> queryByRangeAndInterval(String measurement,
                                                      Map<String, String> tagFilters,
                                                      String field,
                                                      long startTime,
                                                      long endTime,
                                                      long interval) {
        // 1. 构建基础查询条件，获取时间范围内的全量数据
        Query query = new Query();
        query.setMeasurement(measurement);
        query.setTagFilters(tagFilters);
        query.setField(field);
        query.setTimeRange(new TimeRange(startTime, endTime));
        List<Point> allPoints = fileStorageEngine.query(query);

        // 2. 按时间戳排序（保证采样顺序）
        List<Point> sortedPoints = allPoints.stream()
                .sorted(Comparator.comparingLong(Point::getTimestamp))
                .collect(Collectors.toList());

        // 3. 按间隔采样（默认取每个间隔内最后一条数据）
        List<SamplingData> samplingList = new ArrayList<>();
        long currentTime = startTime;
        int pointIndex = 0;
        int pointCount = sortedPoints.size();

        while (currentTime <= endTime) {
            // 确定当前采样间隔的结束时间
            long intervalEnd = currentTime + interval;
            SamplingData samplingData = new SamplingData();
            samplingData.setTimestamp(currentTime);
            samplingData.setValue(null); // 默认值

            // 遍历当前间隔内的所有数据，取最后一条
            for (; pointIndex < pointCount; pointIndex++) {
                Point point = sortedPoints.get(pointIndex);
                long pointTime = point.getTimestamp();

                if (pointTime < currentTime) {
                    continue; // 早于当前采样起点，跳过
                }
                if (pointTime >= intervalEnd) {
                    break; // 超出当前间隔，退出循环
                }

                // 更新为当前间隔内的最新值
                samplingData.setValue(point.getFields().get(field));
            }

            samplingList.add(samplingData);
            // 进入下一个采样间隔
            currentTime += interval;
        }

        return samplingList;
    }

    @Override
    public SamplingData queryByPointInTime(String measurement,
                                           Map<String, String> tagFilters,
                                           String field,
                                           long targetTime,
                                           long timeTolerance) {
        // 1. 构建时间范围：targetTime ± timeTolerance
        long startTime = targetTime - timeTolerance;
        long endTime = targetTime + timeTolerance;

        // 2. 查询该时间范围内的所有数据
        Query query = new Query();
        query.setMeasurement(measurement);
        query.setTagFilters(tagFilters);
        query.setField(field);
        query.setTimeRange(new TimeRange(startTime, endTime));
        List<Point> points = fileStorageEngine.query(query);

        if (points.isEmpty()) {
            return new SamplingData(targetTime, null); // 无匹配数据
        }

        // 3. 找到最接近targetTime的那条数据
        Point closestPoint = null;
        long minDiff = Long.MAX_VALUE;

        for (Point point : points) {
            long diff = Math.abs(point.getTimestamp() - targetTime);
            if (diff < minDiff) {
                minDiff = diff;
                closestPoint = point;
            }
        }

        // 4. 封装结果
        if (closestPoint != null) {
            return new SamplingData(closestPoint.getTimestamp(),
                    closestPoint.getFields().get(field));
        } else {
            return new SamplingData(targetTime, null);
        }
    }
}