// TagIndex.java - 完整修复版
package com.timeseries.db.core;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class TagIndex {
    private final String tagKey;
    private final Map<String, Set<Long>> valueIndex;
    private final Map<String, long[]> rangeCache; // 缓存时间范围，提升查询性能

    public TagIndex(String tagKey) {
        this.tagKey = tagKey;
        this.valueIndex = new ConcurrentHashMap<>();
        this.rangeCache = new ConcurrentHashMap<>();
    }

    /**
     * 添加索引 - 优化版本（推荐）
     * 使用 ConcurrentHashMap.newKeySet() 创建线程安全的 Set
     */
    public void addIndex(String tagValue, long timestamp) {
        Set<Long> timestamps = valueIndex.computeIfAbsent(tagValue,
                k -> ConcurrentHashMap.newKeySet()); // JDK 8 支持

        timestamps.add(timestamp);

        // 更新范围缓存
        updateRangeCache(tagValue, timestamp);
    }

    /**
     * 批量添加索引 - 提升性能
     */
    public void addIndexBatch(String tagValue, Collection<Long> timestamps) {
        if (timestamps == null || timestamps.isEmpty()) {
            return;
        }

        Set<Long> existingSet = valueIndex.computeIfAbsent(tagValue,
                k -> ConcurrentHashMap.newKeySet());

        existingSet.addAll(timestamps);

        // 更新范围缓存
        if (!timestamps.isEmpty()) {
            long min = Collections.min(timestamps);
            long max = Collections.max(timestamps);
            updateRangeCache(tagValue, min, max);
        }
    }

    /**
     * 获取某个标签值对应的时间戳集合
     */
    public Set<Long> getTimestampsForValue(String tagValue) {
        Set<Long> result = valueIndex.get(tagValue);
        return result != null ? Collections.unmodifiableSet(result) : Collections.emptySet();
    }

    /**
     * 获取某个标签值在时间范围内的所有时间戳
     */
    public Set<Long> getTimestampsInRange(String tagValue, long startTime, long endTime) {
        Set<Long> allTimestamps = valueIndex.get(tagValue);
        if (allTimestamps == null || allTimestamps.isEmpty()) {
            return Collections.emptySet();
        }

        // 优化：先检查范围缓存
        long[] range = rangeCache.get(tagValue);
        if (range != null) {
            if (endTime < range[0] || startTime > range[1]) {
                return Collections.emptySet(); // 完全不在范围内
            }
        }

        // 使用 TreeSet 进行范围查询（如果有序的话）
        if (allTimestamps instanceof NavigableSet) {
            NavigableSet<Long> navigableSet = (NavigableSet<Long>) allTimestamps;
            return new TreeSet<>(navigableSet.subSet(startTime, true, endTime, true));
        }

        // 通用实现
        Set<Long> result = new TreeSet<>();
        for (Long timestamp : allTimestamps) {
            if (timestamp >= startTime && timestamp <= endTime) {
                result.add(timestamp);
            }
        }
        return result;
    }

    /**
     * 移除某个时间戳的索引
     */
    public boolean removeIndex(String tagValue, long timestamp) {
        Set<Long> timestamps = valueIndex.get(tagValue);
        if (timestamps != null) {
            boolean removed = timestamps.remove(timestamp);
            if (removed && timestamps.isEmpty()) {
                valueIndex.remove(tagValue);
                rangeCache.remove(tagValue);
            }
            return removed;
        }
        return false;
    }

    /**
     * 获取标签的所有可能值
     */
    public Set<String> getAllTagValues() {
        return Collections.unmodifiableSet(valueIndex.keySet());
    }

    /**
     * 获取标签值的数量
     */
    public int getValueCount() {
        return valueIndex.size();
    }

    /**
     * 获取总索引数量
     */
    public long getTotalIndexCount() {
        long total = 0;
        for (Set<Long> set : valueIndex.values()) {
            total += set.size();
        }
        return total;
    }

    /**
     * 清理旧的索引（基于时间）
     */
    public void cleanupOldIndexes(long cutoffTime) {
        for (Map.Entry<String, Set<Long>> entry : valueIndex.entrySet()) {
            Set<Long> timestamps = entry.getValue();

            // 移除旧的时间戳
            timestamps.removeIf(timestamp -> timestamp < cutoffTime);

            // 如果集合为空，移除整个条目
            if (timestamps.isEmpty()) {
                valueIndex.remove(entry.getKey());
                rangeCache.remove(entry.getKey());
            } else {
                // 更新范围缓存
                updateRangeCacheFromSet(entry.getKey(), timestamps);
            }
        }
    }

    /**
     * 更新范围缓存（单个时间戳）
     */
    private void updateRangeCache(String tagValue, long timestamp) {
        rangeCache.compute(tagValue, (k, existingRange) -> {
            if (existingRange == null) {
                return new long[]{timestamp, timestamp};
            } else {
                if (timestamp < existingRange[0]) {
                    existingRange[0] = timestamp;
                }
                if (timestamp > existingRange[1]) {
                    existingRange[1] = timestamp;
                }
                return existingRange;
            }
        });
    }

    /**
     * 更新范围缓存（指定范围）
     */
    private void updateRangeCache(String tagValue, long min, long max) {
        rangeCache.compute(tagValue, (k, existingRange) -> {
            if (existingRange == null) {
                return new long[]{min, max};
            } else {
                if (min < existingRange[0]) {
                    existingRange[0] = min;
                }
                if (max > existingRange[1]) {
                    existingRange[1] = max;
                }
                return existingRange;
            }
        });
    }

    /**
     * 从集合更新范围缓存
     */
    private void updateRangeCacheFromSet(String tagValue, Set<Long> timestamps) {
        if (timestamps.isEmpty()) {
            rangeCache.remove(tagValue);
            return;
        }

        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;

        for (Long timestamp : timestamps) {
            if (timestamp < min) min = timestamp;
            if (timestamp > max) max = timestamp;
        }

        if (min != Long.MAX_VALUE && max != Long.MIN_VALUE) {
            rangeCache.put(tagValue, new long[]{min, max});
        }
    }

    /**
     * 获取某个标签值的时间范围
     */
    public long[] getTimeRange(String tagValue) {
        long[] cached = rangeCache.get(tagValue);
        if (cached != null) {
            return cached.clone();
        }

        Set<Long> timestamps = valueIndex.get(tagValue);
        if (timestamps == null || timestamps.isEmpty()) {
            return null;
        }

        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;

        for (Long timestamp : timestamps) {
            if (timestamp < min) min = timestamp;
            if (timestamp > max) max = timestamp;
        }

        if (min != Long.MAX_VALUE && max != Long.MIN_VALUE) {
            long[] range = new long[]{min, max};
            rangeCache.put(tagValue, range);
            return range.clone();
        }

        return null;
    }

    /**
     * 清空索引
     */
    public void clear() {
        valueIndex.clear();
        rangeCache.clear();
    }
}