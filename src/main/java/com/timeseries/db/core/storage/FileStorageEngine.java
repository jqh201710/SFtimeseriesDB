// ==================== FileStorageEngine.java（重写） ====================
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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
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

    @Autowired
    private BufferedFileWriterPool writerPool;

    private final ObjectMapper objectMapper = new ObjectMapper();

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
     * 写入时序数据（复用Writer，减少文件打开/关闭）
     */
    public void write(Point point) {
        String filePath = fileNameGenerator.generateFilePath(point);
        writerPool.getWriter(filePath).writeSync(serializePoint(point));
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
            List<String> lines = entry.getValue().stream()
                    .map(this::serializePoint)
                    .collect(Collectors.toList());
            writerPool.getWriter(entry.getKey()).writeBatchSync(lines);
        }

        // 批量使缓存失效
        Set<String> measurements = points.stream()
                .map(Point::getMeasurement)
                .collect(Collectors.toSet());
        measurements.forEach(this::invalidateRelatedCache);
    }

    /**
     * 查询时序数据（带limit保护，防止OOM）
     */
    public List<Point> query(Query query) {
        String measurement = query.getMeasurement();
        Map<String, String> tagFilters = query.getTagFilters();
        TimeRange timeRange = query.getTimeRange();
        String field = query.getField();
        int limit = query.getLimit() != null && query.getLimit() > 0 ? query.getLimit() : Integer.MAX_VALUE;

        // 构建缓存Key（包含field和limit）
        String cacheKey = buildQueryCacheKey(measurement, tagFilters, field, timeRange, limit);

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
                    if (result.size() >= limit) {
                        log.warn("查询结果达到上限 {}，提前终止: measurement={}, field={}", limit, measurement, field);
                        break;
                    }
                }
            } catch (Exception e) {
                log.error("读取文件失败: {}", filePath, e);
            }
            if (result.size() >= limit) break;
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
     * 构建查询缓存Key（包含field、时间范围和limit）
     */
    private String buildQueryCacheKey(String measurement, Map<String, String> tags,
                                      String field, TimeRange timeRange, int limit) {
        String tagStr = "";
        if (tags != null && !tags.isEmpty()) {
            tagStr = "::" + tags.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .map(e -> e.getKey() + "=" + e.getValue())
                    .collect(Collectors.joining("::"));
        }
        return String.format("%s%s::%s::%d::%d::%d",
                measurement, tagStr, field, timeRange.getStart(), timeRange.getEnd(), limit);
    }

    /**
     * 序列化Point，失败时抛异常（不再写入脏数据空行）
     */
    private String serializePoint(Point point) {
        try {
            return objectMapper.writeValueAsString(point);
        } catch (Exception e) {
            log.error("序列化Point失败: {}", point, e);
            throw new RuntimeException("序列化Point失败", e);
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

    /**
     * 列出所有已存在的 measurement（扫描存储目录）
     */
    public List<String> listMeasurements() {
        File baseDir = new File(config.getBasePath());
        if (!baseDir.exists() || !baseDir.isDirectory()) {
            return Collections.emptyList();
        }
        File[] dirs = baseDir.listFiles(File::isDirectory);
        if (dirs == null) return Collections.emptyList();
        return Arrays.stream(dirs)
                .map(File::getName)
                .sorted()
                .collect(Collectors.toList());
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
