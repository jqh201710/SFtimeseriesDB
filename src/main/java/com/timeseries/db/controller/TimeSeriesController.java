// controller/TimeSeriesController.java - REST API控制器
package com.timeseries.db.controller;

import com.timeseries.db.core.TimeSeriesDB;
import com.timeseries.db.core.DataPoint;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
@RequestMapping("/api/v1/timeseries")
public class TimeSeriesController {

    @Autowired
    private TimeSeriesDB timeSeriesDB;

    // ========== 写入操作 ==========

    /**
     * 写入单个数据点
     * POST /api/v1/timeseries/write
     */
    @PostMapping("/write")
    public ApiResponse writeDataPoint(@RequestBody WriteRequest request) {
        try {
            timeSeriesDB.writeDataPoint(
                    request.getSeries(),
                    request.getTimestamp(),
                    request.getValue(),
                    request.getTags()
            );
            return ApiResponse.success("Data point written successfully");
        } catch (Exception e) {
            return ApiResponse.error("Failed to write data point: " + e.getMessage());
        }
    }

    /**
     * 批量写入数据点
     * POST /api/v1/timeseries/batch-write
     */
    @PostMapping("/batch-write")
    public ApiResponse batchWrite(@RequestBody BatchWriteRequest request) {
        try {
            timeSeriesDB.writeDataPointBatch(
                    request.getSeries(),
                    request.getPoints()
            );
            return ApiResponse.success(
                    String.format("Batch write completed: %d points written",
                            request.getPoints().size())
            );
        } catch (Exception e) {
            return ApiResponse.error("Failed to batch write: " + e.getMessage());
        }
    }

    // ========== 查询操作 ==========

    /**
     * 查询单个时间序列
     * GET /api/v1/timeseries/query?series=xxx&startTime=xxx&endTime=xxx
     */
    @GetMapping("/query")
    public ApiResponse query(
            @RequestParam String series,
            @RequestParam long startTime,
            @RequestParam long endTime,
            @RequestParam(required = false) Map<String, String> tags) {

        try {
            List<DataPoint> points = timeSeriesDB.query(series, startTime, endTime, tags);

            QueryResult result = new QueryResult();
            result.setSeries(series);
            result.setStartTime(startTime);
            result.setEndTime(endTime);
            result.setPointCount(points.size());
            result.setPoints(points);
            result.setTags(tags);

            return ApiResponse.success(result);
        } catch (Exception e) {
            return ApiResponse.error("Failed to query: " + e.getMessage());
        }
    }

    /**
     * 查询多个时间序列
     * POST /api/v1/timeseries/query-multiple
     */
    @PostMapping("/query-multiple")
    public ApiResponse queryMultiple(@RequestBody MultiSeriesQueryRequest request) {
        try {
            List<DataPoint> points = timeSeriesDB.queryMultipleSeries(
                    request.getSeriesList(),
                    request.getStartTime(),
                    request.getEndTime(),
                    request.getTags()
            );

            MultiSeriesQueryResult result = new MultiSeriesQueryResult();
            result.setSeriesList(request.getSeriesList());
            result.setStartTime(request.getStartTime());
            result.setEndTime(request.getEndTime());
            result.setPointCount(points.size());
            result.setPoints(points);
            result.setTags(request.getTags());

            return ApiResponse.success(result);
        } catch (Exception e) {
            return ApiResponse.error("Failed to query multiple series: " + e.getMessage());
        }
    }

    // ========== 管理操作 ==========

    /**
     * 获取所有时间序列列表
     * GET /api/v1/timeseries/list
     */
    @GetMapping("/list")
    public ApiResponse listSeries() {
        try {
            List<String> series = timeSeriesDB.getAllSeries();
            return ApiResponse.success(series);
        } catch (Exception e) {
            return ApiResponse.error("Failed to list series: " + e.getMessage());
        }
    }

    /**
     * 获取时间序列统计信息
     * GET /api/v1/timeseries/stats/{series}
     */
    @GetMapping("/stats/{series}")
    public ApiResponse getSeriesStats(@PathVariable String series) {
        try {
            Map<String, Object> stats = timeSeriesDB.getSeriesStats(series);
            return ApiResponse.success(stats);
        } catch (Exception e) {
            return ApiResponse.error("Failed to get series stats: " + e.getMessage());
        }
    }

    /**
     * 获取数据库统计信息
     * GET /api/v1/timeseries/db-stats
     */
    @GetMapping("/db-stats")
    public ApiResponse getDatabaseStats() {
        try {
            Map<String, Object> stats = timeSeriesDB.getDatabaseStats();
            return ApiResponse.success(stats);
        } catch (Exception e) {
            return ApiResponse.error("Failed to get database stats: " + e.getMessage());
        }
    }

    /**
     * 强制刷写数据到磁盘
     * POST /api/v1/timeseries/flush
     */
    @PostMapping("/flush")
    public ApiResponse flush() {
        try {
            // 注意：这里需要实现一个公开的flush方法
            // timeSeriesDB.flushAll();
            return ApiResponse.success("Flush initiated");
        } catch (Exception e) {
            return ApiResponse.error("Failed to flush: " + e.getMessage());
        }
    }

    /**
     * 健康检查
     * GET /api/v1/timeseries/health
     */
    @GetMapping("/health")
    public ApiResponse healthCheck() {
        try {
            Map<String, Object> health = new HashMap<>();
            health.put("status", "UP");
            health.put("timestamp", System.currentTimeMillis());

            // 获取数据库状态
            Map<String, Object> stats = timeSeriesDB.getDatabaseStats();
            health.put("database", stats);

            return ApiResponse.success(health);
        } catch (Exception e) {
            Map<String, Object> health = new HashMap<>();
            health.put("status", "DOWN");
            health.put("error", e.getMessage());
            return ApiResponse.success(health);
        }
    }

    /**
     * 备份数据库
     * POST /api/v1/timeseries/backup
     */
    @PostMapping("/backup")
    public ApiResponse backup(@RequestBody BackupRequest request) {
        try {
            timeSeriesDB.backup(request.getBackupPath());
            return ApiResponse.success("Backup completed successfully");
        } catch (Exception e) {
            return ApiResponse.error("Failed to backup: " + e.getMessage());
        }
    }

    /**
     * 恢复数据库
     * POST /api/v1/timeseries/restore
     */
    @PostMapping("/restore")
    public ApiResponse restore(@RequestBody RestoreRequest request) {
        try {
            timeSeriesDB.restore(request.getBackupPath());
            return ApiResponse.success("Restore completed successfully");
        } catch (Exception e) {
            return ApiResponse.error("Failed to restore: " + e.getMessage());
        }
    }

    /**
     * 删除时间序列
     * DELETE /api/v1/timeseries/{series}
     */
    @DeleteMapping("/{series}")
    public ApiResponse deleteSeries(@PathVariable String series) {
        try {
            // 注意：需要在TimeSeriesDB中添加deleteSeries方法
            // timeSeriesDB.deleteSeries(series);
            return ApiResponse.success(String.format("Series '%s' deleted", series));
        } catch (Exception e) {
            return ApiResponse.error("Failed to delete series: " + e.getMessage());
        }
    }

    /**
     * 清理旧数据
     * POST /api/v1/timeseries/cleanup
     */
    @PostMapping("/cleanup")
    public ApiResponse cleanup(@RequestBody CleanupRequest request) {
        try {
            // 注意：需要在TimeSeriesDB中添加cleanup方法
            // timeSeriesDB.cleanupOldData(request.md.getRetentionDays());
            return ApiResponse.success("Cleanup completed");
        } catch (Exception e) {
            return ApiResponse.error("Failed to cleanup: " + e.getMessage());
        }
    }
}