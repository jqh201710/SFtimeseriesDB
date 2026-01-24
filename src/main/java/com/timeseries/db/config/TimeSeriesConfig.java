package com.timeseries.db.config;

// TimeSeriesConfig.java - 配置类

import com.timeseries.db.core.TimeSeriesDB;
import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PreDestroy;
import java.util.Map;

@Configuration
public class TimeSeriesConfig {

    @Value("${timeseries.data-dir:./data/tsdb}")
    private String dataDir;

    @Value("${timeseries.max-cache-size:10000}")
    private int maxCacheSize;

    @Value("${timeseries.enable-compression:true}")
    private boolean enableCompression;

    private TimeSeriesDB timeSeriesDB;

    @Bean
    public TimeSeriesDB timeSeriesDB() {
        timeSeriesDB = new TimeSeriesDB(dataDir, maxCacheSize, enableCompression);
        return timeSeriesDB;
    }

    @PreDestroy
    public void cleanup() {
        if (timeSeriesDB != null) {
            timeSeriesDB.close();
        }
    }
}

// 请求响应类
@Data
class WriteRequest {
    private String series;
    private long timestamp;
    private double value;
    private Map<String, String> tags;

    // getters and setters
}

@Data
class ApiResponse {
    private boolean success;
    private String message;
    private Object data;

    public static ApiResponse success(Object data) {
        ApiResponse response = new ApiResponse();
        response.setSuccess(true);
        response.setData(data);
        return response;
    }

    public static ApiResponse error(String message) {
        ApiResponse response = new ApiResponse();
        response.setSuccess(false);
        response.setMessage(message);
        return response;
    }

    // getters and setters
}
