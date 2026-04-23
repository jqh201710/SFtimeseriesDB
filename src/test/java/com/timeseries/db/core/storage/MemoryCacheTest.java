package com.timeseries.db.core.storage;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.timeseries.db.core.model.Point;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * MemoryCache 测试：验证缓存功能及内存泄漏修复
 */
public class MemoryCacheTest {

    private MemoryCache memoryCache;

    @BeforeEach
    public void setUp() throws Exception {
        memoryCache = new MemoryCache();
        Cache<String, java.util.List<Point>> cache = CacheBuilder.newBuilder()
                .maximumSize(100)
                .expireAfterWrite(10, TimeUnit.MINUTES)
                .build();
        Field cacheField = MemoryCache.class.getDeclaredField("cache");
        cacheField.setAccessible(true);
        cacheField.set(memoryCache, cache);
    }

    @Test
    public void testPutAndGet() {
        String key = "cpu::value::1000::2000";
        java.util.List<Point> points = Collections.singletonList(createPoint("cpu", 1500L));

        memoryCache.put(key, points);
        java.util.List<Point> cached = memoryCache.get(key);

        assertNotNull(cached);
        assertEquals(1, cached.size());
        assertEquals("cpu", cached.get(0).getMeasurement());
    }

    @Test
    public void testInvalidateByPrefixRemovesEntry() {
        String key1 = "cpu::value::1000::2000";
        String key2 = "cpu::value::2000::3000";
        String key3 = "mem::value::1000::2000";

        memoryCache.put(key1, Collections.singletonList(createPoint("cpu", 1500L)));
        memoryCache.put(key2, Collections.singletonList(createPoint("cpu", 2500L)));
        memoryCache.put(key3, Collections.singletonList(createPoint("mem", 1500L)));

        // 验证索引已建立
        assertEquals(2, memoryCache.getIndexSize("cpu"));
        assertEquals(1, memoryCache.getIndexSize("mem"));

        // 失效 cpu 前缀
        memoryCache.invalidateByPrefix("cpu");

        assertNull(memoryCache.get(key1), "key1 应被失效");
        assertNull(memoryCache.get(key2), "key2 应被失效");
        assertNotNull(memoryCache.get(key3), "key3 不应被失效");
        assertEquals(0, memoryCache.getIndexSize("cpu"), "失效后 cpu 索引 entry 应被彻底移除（修复内存泄漏）");
    }

    @Test
    public void testClear() {
        memoryCache.put("cpu::1000::2000", Collections.singletonList(createPoint("cpu", 1500L)));
        memoryCache.clear();

        assertNull(memoryCache.get("cpu::1000::2000"));
        assertEquals(0, memoryCache.getIndexSize("cpu"));
    }

    private Point createPoint(String measurement, long timestamp) {
        Point point = new Point();
        point.setMeasurement(measurement);
        point.setTags(Collections.emptyMap());
        point.setFields(Collections.emptyMap());
        point.setTimestamp(timestamp);
        return point;
    }
}
