package com.timeseries.db.web;

import com.timeseries.db.core.service.TimeSeriesService;
import com.timeseries.db.web.dto.AdvancedQueryRequest;
import com.timeseries.db.web.dto.AdvancedQueryResponse;
import com.timeseries.db.web.dto.SamplingData;
import com.timeseries.db.web.dto.WriteRequest;
import com.timeseries.db.core.model.Point;
import com.timeseries.db.core.model.Query;
import com.timeseries.db.core.model.TimeRange;
import com.timeseries.db.web.dto.QueryResponse;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/timeseries")
public class TimeSeriesController {

    @Autowired
    private TimeSeriesService timeSeriesService;

    // 原有接口保留（write/batchWrite/query/query-simple）
    @PostMapping("/write")
    public String write(@RequestBody WriteRequest request) {
        Point point = new Point();
        BeanUtils.copyProperties(request, point);
        timeSeriesService.write(point);
        return "success";
    }

    @PostMapping("/batch-write")
    public String batchWrite(@RequestBody List<WriteRequest> requests) {
        List<Point> points = new ArrayList<>();
        for (WriteRequest request : requests) {
            Point point = new Point();
            BeanUtils.copyProperties(request, point);
            points.add(point);
        }
        timeSeriesService.batchWrite(points);
        return "success";
    }

    @PostMapping("/query")
    public QueryResponse query(@RequestBody Query query) {
        List<Point> points = timeSeriesService.query(query);
        QueryResponse response = new QueryResponse();
        response.setCount(points.size());
        response.setData(points);
        return response;
    }

    @GetMapping("/query-simple")
    public QueryResponse querySimple(
            @RequestParam String measurement,
            @RequestParam String tagKey,
            @RequestParam String tagValue,
            @RequestParam String field,
            @RequestParam long start,
            @RequestParam long end) {
        Query query = new Query();
        query.setMeasurement(measurement);

        Map<String, String> tagFilters = new HashMap<>();
        tagFilters.put(tagKey, tagValue);
        query.setTagFilters(tagFilters);

        query.setField(field);
        query.setTimeRange(new TimeRange(start, end));

        List<Point> points = timeSeriesService.query(query);
        QueryResponse response = new QueryResponse();
        response.setCount(points.size());
        response.setData(points);
        return response;
    }

    // ========== 新增高级查询接口 ==========
    /**
     * 高级查询：支持时间范围+间隔采样、指定时间点查询
     */
    @PostMapping("/advanced-query")
    public AdvancedQueryResponse advancedQuery(@RequestBody AdvancedQueryRequest request) {
        AdvancedQueryResponse response = new AdvancedQueryResponse();

        // 1. 范围+间隔采样查询
        if (AdvancedQueryRequest.QueryType.RANGE_INTERVAL.equals(request.getQueryType())) {
            List<SamplingData> samplingList = timeSeriesService.queryByRangeAndInterval(
                    request.getMeasurement(),
                    request.getTagFilters(),
                    request.getField(),
                    request.getStartTime(),
                    request.getEndTime(),
                    request.getInterval()
            );
            response.setTotalCount(samplingList.size());
            response.setSamplingDataList(samplingList);
        }

        // 2. 指定时间点当前值查询
        else if (AdvancedQueryRequest.QueryType.POINT_IN_TIME.equals(request.getQueryType())) {
            SamplingData pointData = timeSeriesService.queryByPointInTime(
                    request.getMeasurement(),
                    request.getTagFilters(),
                    request.getField(),
                    request.getTargetTime(),
                    request.getTimeTolerance()
            );
            response.setTotalCount(pointData.getValue() == null ? 0 : 1);
            response.setPointData(pointData);
        }

        return response;
    }
}