// ==================== FileStorageEngine.java（重写） ====================
package com.timeseries.db.core.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.timeseries.db.config.TimeSeriesConfig;
import com.timeseries.db.core.model.Point;
import com.timeseries.db.core.model.Query;
import com.timeseries.db.core.model.TimeRange;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
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
    private final Map<String, ReentrantLock> fileLocks = new ConcurrentHashMap<>();

    @PostConstruct
    public void init() {
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
     * 写入时序数据（先写文件，再更新缓存）
     */
    public void write(Point point) {
        String filePath = fileNameGenerator.generateFilePath(point);
        appendToFile(filePath, point);
        // 写入后使相关缓存失效（因为新数据可能影响查询结果）
        invalidateRelatedCache(point.getMeasurement());
    }

    /**
     * 批量写入（优化：按文件分组，减少锁竞争）
     */
    public void batchWrite(List<Point> points) {
        if (points == null || points.isEmpty()) {
            return;
        }
        // 按文件路径分组
        Map<String, List<Point>> fileGroups = points.stream()
                .collect(Collectors.groupingBy(fileNameGenerator::generateFilePath));

        // 每组分别写入
        for (Map.Entry<String, List<Point>> entry : fileGroups.entrySet()) {
            appendPointsToFile(entry.getKey(), entry.getValue());
        }

        // 批量使缓存失效
        Set<String> measurements = points.stream()
                .map(Point::getMeasurement)
                .collect(Collectors.toSet());
        measurements.forEach(this::invalidateRelatedCache);
    }

    /**
     * 追加单条数据到文件
     */
    private void appendToFile(String filePath, Point point) {
        ReentrantLock lock = fileLocks.computeIfAbsent(filePath, k -> new ReentrantLock());
        lock.lock();
        try {
            File file = ensureFileExists(filePath);
            // Java 8 兼容写法：FileOutputStream + OutputStreamWriter
            try (FileOutputStream fos = new FileOutputStream(file, true);
                 OutputStreamWriter osw = new OutputStreamWriter(fos, StandardCharsets.UTF_8);
                 BufferedWriter writer = new BufferedWriter(osw)) {
                writer.write(serializePoint(point));
                writer.newLine();
            }
        } catch (Exception e) {
            log.error("写入文件失败: {}", filePath, e);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 批量追加数据到文件
     */
    private void appendPointsToFile(String filePath, List<Point> points) {
        if (points.isEmpty()) return;

        ReentrantLock lock = fileLocks.computeIfAbsent(filePath, k -> new ReentrantLock());
        lock.lock();
        try {
            File file = ensureFileExists(filePath);
            try (FileOutputStream fos = new FileOutputStream(file, true);
                 OutputStreamWriter osw = new OutputStreamWriter(fos, StandardCharsets.UTF_8);
                 BufferedWriter writer = new BufferedWriter(osw)) {
                for (Point point : points) {
                    writer.write(serializePoint(point));
                    writer.newLine();
                }
            }
        } catch (Exception e) {
            log.error("批量写入文件失败: {}", filePath, e);
        } finally {
            lock.unlock();
        }
    }
    private File ensureFileExists(String filePath) throws IOException {
        File file = new File(filePath);
        if (!file.getParentFile().exists()) {
            file.getParentFile().mkdirs();
        }
        if (!file.exists()) {
            file.createNewFile();
        }
        return file;
    }

    /**
     * 查询时序数据
     */
    public List<Point> query(Query query) {
        String measurement = query.getMeasurement();
        Map<String, String> tagFilters = query.getTagFilters();
        TimeRange timeRange = query.getTimeRange();
        String field = query.getField();

        // 构建缓存Key（包含field）
        String cacheKey = buildQueryCacheKey(measurement, tagFilters, field, timeRange);

        // 先查缓存
        List<Point> cached = memoryCache.get(cacheKey);
        if (cached != null) {
            return cached;
        }

        // 查文件
        List<String> filePaths = fileNameGenerator.listFiles(measurement, timeRange);
        List<Point> result = new ArrayList<>();

        for (String filePath : filePaths) {
            File file = new File(filePath);
            if (!file.exists()) continue;

            try (BufferedReader reader = Files.newBufferedReader(Paths.get(filePath), StandardCharsets.UTF_8)) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.trim().isEmpty()) continue;

                    Point point = deserializePoint(line);
                    if (point == null) continue;

                    // 过滤条件
                    if (!matchTags(point.getTags(), tagFilters)) continue;
                    if (point.getTimestamp() < timeRange.getStart()
                            || point.getTimestamp() > timeRange.getEnd()) continue;
                    if (!point.getFields().containsKey(field)) continue;

                    result.add(point);
                }
            } catch (Exception e) {
                log.error("读取文件失败: {}", filePath, e);
            }
        }

        // 按时间排序
        result.sort(Comparator.comparingLong(Point::getTimestamp));

        // 缓存结果
        memoryCache.put(cacheKey, result);
        return result;
    }

    /**
     * 使相关缓存失效
     */
    private void invalidateRelatedCache(String measurement) {
        memoryCache.invalidateByPrefix(measurement);
    }

    /**
     * 构建查询缓存Key（包含field和时间范围）
     */
    private String buildQueryCacheKey(String measurement, Map<String, String> tags,
                                      String field, TimeRange timeRange) {
        String tagStr = "";
        if (tags != null && !tags.isEmpty()) {
            tagStr = "::" + tags.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .map(e -> e.getKey() + "=" + e.getValue())
                    .collect(Collectors.joining("::"));
        }
        return String.format("%s%s::%s::%d::%d",
                measurement, tagStr, field, timeRange.getStart(), timeRange.getEnd());
    }

    private String serializePoint(Point point) {
        try {
            return objectMapper.writeValueAsString(point);
        } catch (Exception e) {
            log.error("序列化Point失败", e);
            return "";
        }
    }

    private Point deserializePoint(String json) {
        try {
            return objectMapper.readValue(json, Point.class);
        } catch (Exception e) {
            log.error("反序列化Point失败: {}", json, e);
            return null;
        }
    }

    private boolean matchTags(Map<String, String> pointTags, Map<String, String> filterTags) {
        if (filterTags == null || filterTags.isEmpty()) return true;
        if (pointTags == null) return false;
        for (Map.Entry<String, String> entry : filterTags.entrySet()) {
            String value = pointTags.get(entry.getKey());
            if (value == null || !value.equals(entry.getValue())) return false;
        }
        return true;
    }
}