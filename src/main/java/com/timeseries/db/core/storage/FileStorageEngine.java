// com.timeseries.db.core.storage.FileStorageEngine.java
package com.timeseries.db.core.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.timeseries.db.config.TimeSeriesConfig;
import com.timeseries.db.core.model.Point;
import com.timeseries.db.core.model.Query;
import com.timeseries.db.core.model.TimeRange;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

@Slf4j
@Component
public class FileStorageEngine {

    @Autowired
    private TimeSeriesConfig config;

    @Autowired
    private MemoryCache memoryCache;

    @Autowired
    private FileNameGenerator fileNameGenerator;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Map<String, ReentrantLock> fileLocks = new HashMap<>(); // 文件写入锁

    @PostConstruct
    public void init() {
        // 创建存储根目录
        File baseDir = new File(config.getBasePath());
        if (!baseDir.exists()) {
            boolean mkdirs = baseDir.mkdirs();
            if (!mkdirs) {
                log.error("创建存储目录失败: {}", config.getBasePath());
                throw new RuntimeException("存储目录创建失败");
            }
        }
        log.info("时序库存储目录初始化完成: {}", config.getBasePath());
    }

    /**
     * 写入时序数据（异步+批量，低内存）
     */
    public void write(Point point) {
        // 1. 生成缓存Key和文件路径
        String cacheKey = buildCacheKey(point);
        String filePath = fileNameGenerator.generateFilePath(point);

        // 2. 先写入缓存（内存）
        List<Point> cachedPoints = memoryCache.get(cacheKey);
        if (cachedPoints == null) {
            cachedPoints = new ArrayList<>();
        }
        cachedPoints.add(point);
        memoryCache.put(cacheKey, cachedPoints);

        // 3. 异步刷盘（避免阻塞，批量写入减少IO）
        new Thread(() -> flushToFile(cacheKey, filePath)).start();
    }

    /**
     * 批量刷盘到文件（内存映射文件提升性能）
     */
    private void flushToFile(String cacheKey, String filePath) {
        ReentrantLock lock = fileLocks.computeIfAbsent(filePath, k -> new ReentrantLock());
        lock.lock();
        try {
            List<Point> points = memoryCache.get(cacheKey);
            if (points == null || points.isEmpty()) {
                return;
            }

            // 创建文件（含父目录）
            File file = new File(filePath);
            if (!file.getParentFile().exists()) {
                file.getParentFile().mkdirs();
            }

            // 使用内存映射文件写入（高性能，低内存）
            try (FileChannel channel = new RandomAccessFile(file, "rw").getChannel()) {
                long fileSize = file.length();
                // 序列化数据（每行一个Point，JSON格式）
                String data = points.stream()
                        .map(this::serializePoint)
                        .collect(Collectors.joining("\n")) + "\n";
                byte[] bytes = data.getBytes(StandardCharsets.UTF_8);

                // 映射文件尾部到内存
                MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, fileSize, bytes.length);
                buffer.put(bytes);
                buffer.force(); // 强制刷盘
            }

            // 刷盘后移除缓存（减少内存占用）
            memoryCache.remove(cacheKey);
            log.debug("刷盘完成: {} 数据量: {}", filePath, points.size());
        } catch (Exception e) {
            log.error("刷盘失败", e);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 查询时序数据（先查缓存，再查文件）
     */
    public List<Point> query(Query query) {
        String measurement = query.getMeasurement();
        Map<String, String> tagFilters = query.getTagFilters();
        TimeRange timeRange = query.getTimeRange();
        String field = query.getField();

        // 1. 生成缓存Key，先查缓存
        String cacheKey = buildCacheKey(measurement, tagFilters);
        List<Point> cachedPoints = memoryCache.get(cacheKey);
        if (cachedPoints != null && !cachedPoints.isEmpty()) {
            // 过滤时间范围和字段
            return cachedPoints.stream()
                    .filter(p -> p.getTimestamp() >= timeRange.getStart() && p.getTimestamp() <= timeRange.getEnd())
                    .filter(p -> p.getFields().containsKey(field))
                    .collect(Collectors.toList());
        }

        // 2. 查文件（按时间分片遍历）
        List<String> filePaths = fileNameGenerator.listFiles(measurement, timeRange);
        List<Point> result = new ArrayList<>();
        for (String filePath : filePaths) {
            File file = new File(filePath);
            if (!file.exists()) {
                continue;
            }

            // 按行读取文件（流式读取，低内存）
            try (BufferedReader reader = Files.newBufferedReader(Paths.get(filePath), StandardCharsets.UTF_8)) {
                String line;
                while ((line = reader.readLine()) != null) {
                    Point point = deserializePoint(line);
                    if (point == null) {
                        continue;
                    }
                    // 过滤标签、时间、字段
                    if (matchTags(point.getTags(), tagFilters)
                            && point.getTimestamp() >= timeRange.getStart()
                            && point.getTimestamp() <= timeRange.getEnd()
                            && point.getFields().containsKey(field)) {
                        result.add(point);
                    }
                }
            } catch (Exception e) {
                log.error("读取文件失败: {}", filePath, e);
            }
        }

        // 3. 缓存查询结果（热点数据加速）
        memoryCache.put(cacheKey, result);
        return result;
    }

    // ========== 工具方法 ==========
    /**
     * 构建缓存Key
     */
    private String buildCacheKey(Point point) {
        return buildCacheKey(point.getMeasurement(), point.getTags());
    }

    private String buildCacheKey(String measurement, Map<String, String> tags) {
        if (tags == null || tags.isEmpty()) {
            return measurement;
        }
        // 标签排序，保证Key唯一
        String tagStr = tags.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(e -> e.getKey() + "=" + e.getValue())
                .collect(Collectors.joining("::"));
        return measurement + "::" + tagStr;
    }

    /**
     * 序列化Point为JSON字符串
     */
    private String serializePoint(Point point) {
        try {
            return objectMapper.writeValueAsString(point);
        } catch (Exception e) {
            log.error("序列化Point失败", e);
            return "";
        }
    }

    /**
     * 反序列化JSON字符串为Point
     */
    private Point deserializePoint(String json) {
        try {
            return objectMapper.readValue(json, Point.class);
        } catch (Exception e) {
            log.error("反序列化Point失败: {}", json, e);
            return null;
        }
    }

    /**
     * 标签匹配
     */
    private boolean matchTags(Map<String, String> pointTags, Map<String, String> filterTags) {
        if (filterTags == null || filterTags.isEmpty()) {
            return true;
        }
        if (pointTags == null) {
            return false;
        }
        for (Map.Entry<String, String> entry : filterTags.entrySet()) {
            String value = pointTags.get(entry.getKey());
            if (value == null || !value.equals(entry.getValue())) {
                return false;
            }
        }
        return true;
    }
}