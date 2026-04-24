package com.timeseries.db.web;

import com.timeseries.db.core.model.Point;
import com.timeseries.db.core.model.Query;
import com.timeseries.db.core.model.TimeRange;
import com.timeseries.db.core.service.TimeSeriesService;
import com.timeseries.db.web.dto.MultiMetricQueryResponse;
import com.timeseries.db.web.dto.SamplingData;
import com.timeseries.db.web.dto.SimpleQueryResponse;
import com.timeseries.db.web.dto.SimpleWriteRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.*;

/**
 * 极简版时序库API：仅保留measurement、value、timestamp
 * 无需传入tags，内部自动处理
 */
@RestController
@RequestMapping("/api/timeseries")
public class SimpleTimeSeriesController {

    @Autowired
    private TimeSeriesService timeSeriesService;

    /**
     * 【极简写入】仅传measurement、value、timestamp
     * POST http://localhost:8080/api/simple/write
     */
    @PostMapping("/write")
    public String simpleWrite(@RequestBody SimpleWriteRequest request) {
        // 1. 构建Point对象，自动填充空tags，固定字段名为"value"
        Point point = new Point();
        point.setMeasurement(request.getMeasurement());
        point.setTags(new HashMap<>()); // 空tags
        // 固定fields的key为"value"，值为传入的value
        Map<String, Object> fields = new HashMap<>();
        fields.put("value", request.getValue());
        point.setFields(fields);
        point.setTimestamp(request.getTimestamp());

        // 2. 调用原有服务写入
        timeSeriesService.write(point);
        return "success";
    }
    // 在SimpleTimeSeriesController中新增
    /**
     * 极简批量写入：一次性写入多个指标的数据
     * POST http://localhost:8080/api/simple/batch-write
     */
    @PostMapping("/batch-write")
    public String simpleBatchWrite(@RequestBody List<SimpleWriteRequest> requests) {
        List<Point> points = new ArrayList<>();
        for (SimpleWriteRequest request : requests) {
            Point point = new Point();
            point.setMeasurement(request.getMeasurement());
            point.setTags(new HashMap<>());
            Map<String, Object> fields = new HashMap<>();
            fields.put("value", request.getValue());
            point.setFields(fields);
            point.setTimestamp(request.getTimestamp());
            points.add(point);
        }
        timeSeriesService.batchWrite(points);
        return "success";
    }

    /**
     * 【极简查询-实时值】按measurement+指定时间戳查值
     * GET http://localhost:8080/api/simple/query/point?measurement=cpu_usage&targetTime=1737724800000
     */
    @GetMapping("/query/point")
    public SimpleQueryResponse querySinglePoint(
            @RequestParam String measurement,
            @RequestParam long targetTime) {
        // 1. 调用原有服务查询指定时间点的值（误差±1秒）
        SimpleQueryResponse response = new SimpleQueryResponse();
        SimpleQueryResponse.SimpleDataPoint dataPoint = new SimpleQueryResponse.SimpleDataPoint();

        // 构建查询条件：空tags，字段固定为"value"
        Map<String, String> emptyTags = new HashMap<>();
        SamplingData pointData = timeSeriesService.queryByPointInTime(
                measurement, emptyTags, "value", targetTime, 1000L);

        // 2. 封装极简响应
        dataPoint.setTimestamp(pointData.getTimestamp());
        dataPoint.setValue(pointData.getValue());
        response.setSinglePoint(dataPoint);
        return response;
    }

    /**
     * 【极简查询-范围】按measurement+时间范围查值
     * GET http://localhost:8080/api/simple/query/range?measurement=cpu_usage&start=1737724800000&end=1737728400000
     */
    @GetMapping("/query/range")
    public SimpleQueryResponse queryRange(
            @RequestParam String measurement,
            @RequestParam long start,
            @RequestParam long end) {
        // 1. 构建基础查询条件
        Query query = new Query();
        query.setMeasurement(measurement);
        query.setTagFilters(new HashMap<>()); // 空tags
        query.setField("value");
        query.setTimeRange(new TimeRange(start, end));

        // 2. 查询数据并封装极简响应
        List<Point> points = timeSeriesService.query(query);
        SimpleQueryResponse response = new SimpleQueryResponse();
        List<SimpleQueryResponse.SimpleDataPoint> dataPoints = new ArrayList<>();

        for (Point point : points) {
            SimpleQueryResponse.SimpleDataPoint dp = new SimpleQueryResponse.SimpleDataPoint();
            dp.setTimestamp(point.getTimestamp());
            dp.setValue(point.getFields().get("value"));
            dataPoints.add(dp);
        }

        response.setDataPoints(dataPoints);
        return response;
    }

    /**
     * 获取所有已存在的指标列表（动态扫描存储目录）
     * GET http://localhost:8080/api/timeseries/measurements
     */
    @GetMapping("/measurements")
    public List<String> listMeasurements() {
        return timeSeriesService.listMeasurements();
    }

    // ========== 新增：多组数据（多指标）查询接口 ==========
    /**
     * 极简多指标查询：一次性查多个measurement的时间范围数据
     * GET http://localhost:8080/api/simple/query/multi?measurements=cpu_usage,memory_usage&start=1737724800000&end=1737728400000
     */
    @GetMapping("/query/multi")
    public MultiMetricQueryResponse queryMultiMetric(
            @RequestParam String measurements, // 多个指标名，用逗号分隔（如cpu_usage,memory_usage）
            @RequestParam long start,          // 开始时间戳（毫秒）
            @RequestParam long end) {          // 结束时间戳（毫秒）

        // 1. 解析多个measurement（逗号分隔转列表）
        List<String> measurementList = Arrays.asList(measurements.split(","));
        // 2. 构建返回结果：按指标名分组
        Map<String, List<SimpleQueryResponse.SimpleDataPoint>> metricDataMap = new HashMap<>();

        // 3. 批量查询每个指标的数据
        for (String measurement : measurementList) {
            // 构建单个指标的查询条件
            Query query = new Query();
            query.setMeasurement(measurement.trim()); // 去除空格，兼容"cpu_usage, memory_usage"格式
            query.setTagFilters(new HashMap<>());
            query.setField("value");
            query.setTimeRange(new TimeRange(start, end));

            // 查询数据并封装
            List<Point> points = timeSeriesService.query(query);
            List<SimpleQueryResponse.SimpleDataPoint> dataPoints = new ArrayList<>();
            for (Point point : points) {
                SimpleQueryResponse.SimpleDataPoint dp = new SimpleQueryResponse.SimpleDataPoint();
                dp.setTimestamp(point.getTimestamp());
                dp.setValue(point.getFields().get("value"));
                dataPoints.add(dp);
            }

            // 放入结果Map
            metricDataMap.put(measurement.trim(), dataPoints);
        }

        // 4. 封装响应（修复：使用包装对象，与客户端契约一致）
        MultiMetricQueryResponse response = new MultiMetricQueryResponse();
        response.setMetricData(metricDataMap);
        return response;
    }
}
