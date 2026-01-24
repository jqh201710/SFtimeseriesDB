// com.timeseries.db.core.storage.MemoryCache.java
package com.timeseries.db.core.storage;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.timeseries.db.config.TimeSeriesConfig;
import com.timeseries.db.core.model.Point;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 轻量级LRU缓存：仅缓存最近热点数据，控制内存占用
 * 缓存Key格式：measurement::tag1=value1::tag2=value2
 */
@Component
public class MemoryCache {

    @Autowired
    private TimeSeriesConfig config;

    private Cache<String, List<Point>> cache;

    @PostConstruct
    public void init() {
        TimeSeriesConfig.CacheConfig cacheConfig = config.getCache();
        // 初始化LRU缓存：设置最大容量+过期时间
        cache = CacheBuilder.newBuilder()
                .maximumSize(cacheConfig.getMaxSize())
                .expireAfterWrite(cacheConfig.getExpireMinutes(), TimeUnit.MINUTES)
                .concurrencyLevel(Runtime.getRuntime().availableProcessors())
                .build();
    }

    /**
     * 添加缓存
     */
    public void put(String key, List<Point> points) {
        cache.put(key, points);
    }

    /**
     * 获取缓存
     */
    public List<Point> get(String key) {
        return cache.getIfPresent(key);
    }

    /**
     * 移除缓存
     */
    public void remove(String key) {
        cache.invalidate(key);
    }

    /**
     * 清理所有缓存
     */
    public void clear() {
        cache.invalidateAll();
    }
}