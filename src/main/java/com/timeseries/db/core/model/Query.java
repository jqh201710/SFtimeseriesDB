// com.timeseries.db.core.model/Query.java
package com.timeseries.db.core.model;

import lombok.Data;

import java.util.Map;

@Data
public class Query {
    private String measurement;
    private Map<String, String> tagFilters; // 标签过滤
    private TimeRange timeRange;            // 时间范围
    private String field;                   // 要查询的字段
    /** 最大返回数据点数，防止OOM，默认10000，设为null或负数表示不限制 */
    private Integer limit = 10000;
}
