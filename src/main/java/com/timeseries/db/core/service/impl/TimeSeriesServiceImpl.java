// ==================== TimeSeriesServiceImpl.java（修复采样逻辑） ====================
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

    @Override
    public void write(Point point) {
        fileStorageEngine.write(point);
    }

    @Override
    public void batchWrite(List<Point> points) {
        fileStorageEngine.batchWrite(points);
    }

    @Override
    public List<Point> query(Query query) {
        return fileStorageEngine.query(query);
    }

    @Override
    public List<SamplingData> queryByRangeAndInterval(String measurement,
                                                      Map<String, String> tagFilters,
                                                      String field,
                                                      long startTime,
                                                      long endTime,
                                                      long interval) {
        // 1. 查询全量数据
        Query query = new Query();
        query.setMeasurement(measurement);
        query.setTagFilters(tagFilters != null ? tagFilters : new HashMap<>());
        query.setField(field);
        query.setTimeRange(new TimeRange(startTime, endTime));
        List<Point> allPoints = fileStorageEngine.query(query);

        if (allPoints.isEmpty()) {
            // 返回空采样点
            List<SamplingData> emptyResult = new ArrayList<>();
            for (long t = startTime; t <= endTime; t += interval) {
                emptyResult.add(new SamplingData(t, null));
            }
            return emptyResult;
        }

        // 2. 按时间排序
        List<Point> sortedPoints = allPoints.stream()
                .sorted(Comparator.comparingLong(Point::getTimestamp))
                .collect(Collectors.toList());

        // 3. 按间隔采样 - 修复后的逻辑
        List<SamplingData> samplingList = new ArrayList<>();
        int pointIndex = 0;
        int pointCount = sortedPoints.size();

        for (long windowStart = startTime; windowStart <= endTime; windowStart += interval) {
            long windowEnd = windowStart + interval;
            SamplingData samplingData = new SamplingData();
            samplingData.setTimestamp(windowStart);

            // 找当前窗口内最后一条数据
            Point lastInWindow = null;
            while (pointIndex < pointCount) {
                Point point = sortedPoints.get(pointIndex);
                long pointTime = point.getTimestamp();

                if (pointTime < windowStart) {
                    pointIndex++;
                    continue;
                }
                if (pointTime >= windowEnd) {
                    // 超出当前窗口，不移动索引，留给下一个窗口
                    break;
                }
                // 在当前窗口内
                lastInWindow = point;
                pointIndex++;
            }

            if (lastInWindow != null) {
                samplingData.setValue(lastInWindow.getFields().get(field));
            } else {
                // 如果当前窗口没有数据，尝试用前一个窗口的最后一条数据填充（可选）
                samplingData.setValue(null);
            }

            samplingList.add(samplingData);
        }

        return samplingList;
    }

    @Override
    public SamplingData queryByPointInTime(String measurement,
                                           Map<String, String> tagFilters,
                                           String field,
                                           long targetTime,
                                           long timeTolerance) {
        long startTime = targetTime - timeTolerance;
        long endTime = targetTime + timeTolerance;

        Query query = new Query();
        query.setMeasurement(measurement);
        query.setTagFilters(tagFilters != null ? tagFilters : new HashMap<>());
        query.setField(field);
        query.setTimeRange(new TimeRange(startTime, endTime));
        List<Point> points = fileStorageEngine.query(query);

        if (points.isEmpty()) {
            return new SamplingData(targetTime, null);
        }

        // 找到最接近targetTime的数据
        Point closestPoint = points.stream()
                .min(Comparator.comparingLong(p -> Math.abs(p.getTimestamp() - targetTime)))
                .orElse(null);

        if (closestPoint != null) {
            return new SamplingData(closestPoint.getTimestamp(),
                    closestPoint.getFields().get(field));
        }
        return new SamplingData(targetTime, null);
    }
}