// ==================== MemoryCache.java（增强） ====================
package com.timeseries.db.core.storage;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.timeseries.db.config.TimeSeriesConfig;
import com.timeseries.db.core.model.Point;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
public class MemoryCache {

    @Autowired
    private TimeSeriesConfig config;

    private Cache<String, List<Point>> cache;

    // 记录measurement对应的所有缓存Key，用于批量失效
    private final ConcurrentHashMap<String, Set<String>> measurementKeyIndex = new ConcurrentHashMap<>();

    @PostConstruct
    public void init() {
        TimeSeriesConfig.CacheConfig cacheConfig = config.getCache();
        cache = CacheBuilder.newBuilder()
                .maximumSize(cacheConfig.getMaxSize())
                .expireAfterWrite(cacheConfig.getExpireMinutes(), TimeUnit.MINUTES)
                .concurrencyLevel(Runtime.getRuntime().availableProcessors())
                .removalListener(notification -> {
                    // 缓存移除时清理索引（简化处理，实际可优化）
                })
                .build();
    }

    public void put(String key, List<Point> points) {
        cache.put(key, points);
        // 提取measurement并建立索引
        String measurement = extractMeasurement(key);
        measurementKeyIndex.computeIfAbsent(measurement, k ->
                java.util.Collections.newSetFromMap(new ConcurrentHashMap<>())).add(key);
    }

    public List<Point> get(String key) {
        return cache.getIfPresent(key);
    }

    public void remove(String key) {
        cache.invalidate(key);
    }

    public void clear() {
        cache.invalidateAll();
        measurementKeyIndex.clear();
    }

    /**
     * 按measurement前缀失效缓存（用于写入后刷新）
     */
    public void invalidateByPrefix(String measurement) {
        Set<String> keys = measurementKeyIndex.get(measurement);
        if (keys != null) {
            for (String key : keys) {
                cache.invalidate(key);
            }
            keys.clear();
            measurementKeyIndex.remove(measurement); // 修复内存泄漏：彻底移除entry
        }
    }

    private String extractMeasurement(String key) {
        // key格式: measurement::tags::field::start::end:limit 或 measurement::field::start::end:limit
        int idx = key.indexOf("::");
        return idx > 0 ? key.substring(0, idx) : key;
    }

    /**
     * 测试辅助：获取指定 measurement 的索引 key 数量
     */
    int getIndexSize(String measurement) {
        java.util.Set<String> keys = measurementKeyIndex.get(measurement);
        return keys == null ? 0 : keys.size();
    }
}
