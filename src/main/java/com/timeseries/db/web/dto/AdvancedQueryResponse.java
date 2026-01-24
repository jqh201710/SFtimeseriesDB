package com.timeseries.db.web.dto;

import lombok.Data;

import java.util.List;

/**
 * 高级查询响应DTO
 */
@Data
public class AdvancedQueryResponse {
    /** 数据总数 */
    private int totalCount;
    /** 采样结果（范围+间隔查询时返回） */
    private List<SamplingData> samplingDataList;
    /** 时间点当前值（指定时间点查询时返回） */
    private SamplingData pointData;
}