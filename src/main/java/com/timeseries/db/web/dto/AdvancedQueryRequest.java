package com.timeseries.db.web.dto;

import lombok.Data;

import java.util.Map;

/**
 * 高级查询请求DTO：支持时间范围+间隔采样、指定时间点查询
 */
@Data
public class AdvancedQueryRequest {
    /** 基础参数：测量项 */
    private String measurement;
    /** 基础参数：标签过滤 */
    private Map<String, String> tagFilters;
    /** 基础参数：要查询的字段 */
    private String field;

    // ========== 时间范围+间隔采样参数 ==========
    /** 开始时间戳（毫秒） */
    private long startTime;
    /** 结束时间戳（毫秒） */
    private long endTime;
    /** 采样间隔（毫秒），比如60000=1分钟，300000=5分钟 */
    private long interval;

    // ========== 指定时间点查询参数 ==========
    /** 目标时间戳（毫秒）：单独查询该时间点的当前值时使用 */
    private long targetTime;
    /** 时间误差范围（毫秒）：默认1000=1秒，即查找targetTime±1秒内的数据 */
    private long timeTolerance = 1000L;

    /** 查询类型：RANGE_INTERVAL（范围+间隔）、POINT_IN_TIME（指定时间点） */
    private QueryType queryType;

    /**
     * 查询类型枚举
     */
    public enum QueryType {
        RANGE_INTERVAL, POINT_IN_TIME
    }
}