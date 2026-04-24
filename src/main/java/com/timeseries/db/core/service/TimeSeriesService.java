// com.timeseries.db.core.service.TimeSeriesService.java
package com.timeseries.db.core.service;

import com.timeseries.db.core.model.Point;
import com.timeseries.db.core.model.Query;
import com.timeseries.db.web.dto.SamplingData;

import java.util.List;
import java.util.Map;

public interface TimeSeriesService {
    /**
     * 写入时序数据
     */
    void write(Point point);

    /**
     * 批量写入
     */
    void batchWrite(List<Point> points);

    /**
     * 查询时序数据
     */
    List<Point> query(Query query);

    // ========== 新增扩展方法 ==========
    /**
     * 按时间范围+间隔采样查询
     * @param measurement 测量项
     * @param tagFilters 标签过滤
     * @param field 字段名
     * @param startTime 开始时间
     * @param endTime 结束时间
     * @param interval 采样间隔（毫秒）
     * @return 采样数据列表
     */
    List<SamplingData> queryByRangeAndInterval(String measurement,
                                               Map<String, String> tagFilters,
                                               String field,
                                               long startTime,
                                               long endTime,
                                               long interval);

    /**
     * 查询指定时间点的当前值
     * @param measurement 测量项
     * @param tagFilters 标签过滤
     * @param field 字段名
     * @param targetTime 目标时间
     * @param timeTolerance 时间误差范围（±）
     * @return 该时间点的数值（最接近的一条）
     */
    SamplingData queryByPointInTime(String measurement,
                                    Map<String, String> tagFilters,
                                    String field,
                                    long targetTime,
                                    long timeTolerance);

    /**
     * 列出所有已存在的 measurement
     * @return 按字母序排列的指标名列表
     */
    List<String> listMeasurements();
}
