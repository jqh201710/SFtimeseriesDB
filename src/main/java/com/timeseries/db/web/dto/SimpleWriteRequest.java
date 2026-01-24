// 极简写入请求DTO
package com.timeseries.db.web.dto;

import lombok.Data;

@Data
public class SimpleWriteRequest {
    /** 指标名（如cpu_usage、memory_usage） */
    private String measurement;
    /** 指标数值（如CPU使用率0.85、内存使用量1024） */
    private Object value;
    /** 时间戳（毫秒） */
    private long timestamp;
}
