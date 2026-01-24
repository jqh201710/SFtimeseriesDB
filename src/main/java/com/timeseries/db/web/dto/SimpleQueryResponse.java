// 极简查询响应DTO
package com.timeseries.db.web.dto;

import lombok.Data;

import java.util.List;

@Data
public class SimpleQueryResponse {
    /** 数据点列表（范围查询返回） */
    private List<SimpleDataPoint> dataPoints;
    /** 单个数据点（实时值查询返回） */
    private SimpleDataPoint singlePoint;

    // 内部静态类：极简数据点
    @Data
    public static class SimpleDataPoint {
        private long timestamp;
        private Object value;
    }
}