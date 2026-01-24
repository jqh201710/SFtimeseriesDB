// com.timeseries.db.config.TimeSeriesConfig.java
package com.timeseries.db.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties(prefix = "timeseries.storage")
public class TimeSeriesConfig {
    /** 存储根路径 */
    private String basePath = "./timeseries-data";
    /** 分片类型：HOUR/DAY */
    private String shardType = "HOUR";
    /** 缓存配置 */
    private CacheConfig cache = new CacheConfig();
    /** 异步线程池配置 */
    private AsyncConfig async = new AsyncConfig();

    @Data
    public static class CacheConfig {
        /** 缓存最大数据点数 */
        private int maxSize = 10000;
        /** 缓存过期分钟数 */
        private int expireMinutes = 10;
    }

    @Data
    public static class AsyncConfig {
        private int corePoolSize = 4;
        private int maxPoolSize = 8;
        private int queueCapacity = 1000;
    }
}