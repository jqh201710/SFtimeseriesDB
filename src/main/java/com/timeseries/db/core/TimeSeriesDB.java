package com.timeseries.db.core;

import com.timeseries.db.storage.DataFile;
import com.timeseries.db.storage.IndexFile;
import com.timeseries.db.storage.MetaFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class TimeSeriesDB {
    private static final Logger logger = LoggerFactory.getLogger(TimeSeriesDB.class);

    private final String dataDir;
    private final Map<String, TimeSeries> seriesCache;
    private final ExecutorService flushExecutor;
    private final ScheduledExecutorService compactionExecutor;
    private final ScheduledExecutorService cleanupExecutor;
    private final ReadWriteLock globalLock;
    private final int maxCacheSize;
    private final boolean enableCompression;
    private final long maxMemoryPoints;

    // 统计信息
    private final AtomicLong totalWrites = new AtomicLong(0);
    private final AtomicLong totalQueries = new AtomicLong(0);
    private final AtomicLong totalFlushes = new AtomicLong(0);
    private final AtomicLong totalCompactions = new AtomicLong(0);

    // 元数据管理
    private final MetaFile metaFile;

    // 配置常量
    private static final int DEFAULT_FLUSH_INTERVAL_SECONDS = 30;
    private static final int DEFAULT_COMPACTION_INTERVAL_HOURS = 1;
    private static final int DEFAULT_RETENTION_DAYS = 30;
    private static final long DEFAULT_MAX_MEMORY_POINTS = 100000L;
    private static final int MAX_FLUSH_THREADS = 4;
    private static final int MAX_COMPACTION_THREADS = 2;

    public TimeSeriesDB(String dataDir, int maxCacheSize, boolean enableCompression) {
        this(dataDir, maxCacheSize, enableCompression, DEFAULT_MAX_MEMORY_POINTS);
    }

    public TimeSeriesDB(String dataDir, int maxCacheSize, boolean enableCompression, long maxMemoryPoints) {
        this.dataDir = dataDir;
        this.maxCacheSize = maxCacheSize;
        this.enableCompression = enableCompression;
        this.maxMemoryPoints = maxMemoryPoints;
        this.seriesCache = new ConcurrentHashMap<>();

        // 创建线程池
        this.flushExecutor = createFlushExecutor();
        this.compactionExecutor = Executors.newSingleThreadScheduledExecutor();
        this.cleanupExecutor = Executors.newSingleThreadScheduledExecutor();
        this.globalLock = new ReentrantReadWriteLock();

        // 初始化元数据
        this.metaFile = new MetaFile(dataDir);

        initDataDir();
        startBackgroundTasks();

        logger.info("TimeSeriesDB initialized with data directory: {}", dataDir);
    }

    private ExecutorService createFlushExecutor() {
        int availableProcessors = Runtime.getRuntime().availableProcessors();
        int threadCount = Math.min(availableProcessors, MAX_FLUSH_THREADS);

        return Executors.newFixedThreadPool(threadCount, new ThreadFactory() {
            private final AtomicInteger counter = new AtomicInteger(0);
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r, "tsdb-flush-" + counter.incrementAndGet());
                thread.setDaemon(true);
                thread.setPriority(Thread.MIN_PRIORITY); // 后台任务，低优先级
                return thread;
            }
        });
    }

    private void initDataDir() {
        try {
            Files.createDirectories(Paths.get(dataDir));
            Files.createDirectories(Paths.get(dataDir, "data"));
            Files.createDirectories(Paths.get(dataDir, "index"));
            Files.createDirectories(Paths.get(dataDir, "meta"));
            Files.createDirectories(Paths.get(dataDir, "temp"));
            Files.createDirectories(Paths.get(dataDir, "wal")); // Write-Ahead Log目录

            logger.debug("Created data directory structure in: {}", dataDir);
        } catch (IOException e) {
            logger.error("Failed to create data directory: {}", dataDir, e);
            throw new RuntimeException("Failed to create data directory", e);
        }
    }

    private void startBackgroundTasks() {
        // 加载配置
        MetaFile.DatabaseConfig config;
        try {
            config = metaFile.loadDatabaseConfig();
        } catch (IOException e) {
            logger.warn("Failed to load database config, using defaults", e);
            config = new MetaFile.DatabaseConfig();
        }

        int flushInterval = config.getFlushIntervalSeconds();
        int compactionInterval = config.getCompactionIntervalHours();

        // 每N秒刷写一次脏数据
        compactionExecutor.scheduleAtFixedRate(() -> {
            try {
                flushDirtySeries();
            } catch (Exception e) {
                logger.error("Error in flushDirtySeries task", e);
            }
        }, flushInterval, flushInterval, TimeUnit.SECONDS);

        // 每M小时压缩一次文件
        compactionExecutor.scheduleAtFixedRate(() -> {
            try {
                compactOldFiles();
            } catch (Exception e) {
                logger.error("Error in compactOldFiles task", e);
            }
        }, compactionInterval, compactionInterval, TimeUnit.HOURS);

        // 每天清理一次过期数据
        MetaFile.DatabaseConfig finalConfig = config;
        cleanupExecutor.scheduleAtFixedRate(() -> {
            try {
                cleanupExpiredData(finalConfig.getRetentionDays());
            } catch (Exception e) {
                logger.error("Error in cleanupExpiredData task", e);
            }
        }, 24, 24, TimeUnit.HOURS);

        // 每小时统计一次
        compactionExecutor.scheduleAtFixedRate(() -> {
            try {
                logStatistics();
            } catch (Exception e) {
                logger.error("Error in logStatistics task", e);
            }
        }, 1, 1, TimeUnit.HOURS);

        logger.info("Background tasks started: flush every {}s, compact every {}h",
                flushInterval, compactionInterval);
    }

    public void writeDataPoint(String series, long timestamp, double value,
                               Map<String, String> tags) {
        globalLock.readLock().lock();
        long startTime = System.currentTimeMillis();

        try {
            TimeSeries ts = seriesCache.computeIfAbsent(series,
                    k -> new TimeSeries(k, dataDir, enableCompression, maxMemoryPoints));

            ts.writePoint(timestamp, value, tags);
            totalWrites.incrementAndGet();

            // 如果内存数据点过多，异步刷写最老的时间序列
            if (getTotalMemoryPoints() > maxMemoryPoints) {
                flushExecutor.submit(() -> {
                    try {
                        flushOldestSeries();
                    } catch (Exception e) {
                        logger.error("Error flushing oldest series", e);
                    }
                });
            }

            long duration = System.currentTimeMillis() - startTime;
            if (duration > 100) { // 慢写入日志
                logger.warn("Slow write detected for series {}: {}ms", series, duration);
            }

        } finally {
            globalLock.readLock().unlock();
        }
    }

    public void writeDataPointBatch(String series, List<DataPoint> points) {
        if (points == null || points.isEmpty()) {
            return;
        }

        globalLock.readLock().lock();
        long startTime = System.currentTimeMillis();

        try {
            TimeSeries ts = seriesCache.computeIfAbsent(series,
                    k -> new TimeSeries(k, dataDir, enableCompression, maxMemoryPoints));

            for (DataPoint point : points) {
                ts.writePoint(point.getTimestamp(), point.getValue(), point.getTags());
            }

            totalWrites.addAndGet(points.size());

            // 批量写入后检查是否需要刷写
            if (getTotalMemoryPoints() > maxMemoryPoints * 0.8) { // 80%阈值
                flushExecutor.submit(() -> {
                    try {
                        flushOldestSeries();
                    } catch (Exception e) {
                        logger.error("Error flushing after batch write", e);
                    }
                });
            }

            long duration = System.currentTimeMillis() - startTime;
            logger.debug("Batch write completed for series {}: {} points in {}ms",
                    series, points.size(), duration);

        } finally {
            globalLock.readLock().unlock();
        }
    }

    public List<DataPoint> query(String series, long startTime, long endTime,
                                 Map<String, String> tagFilters) {
        globalLock.readLock().lock();
        long queryStart = System.currentTimeMillis();
        totalQueries.incrementAndGet();

        try {
            TimeSeries ts = seriesCache.get(series);
            List<DataPoint> result = new ArrayList<>();

            // 先从内存查询
            if (ts != null && ts.hasDataInMemory()) {
                List<DataPoint> memoryResult = ts.queryFromMemory(startTime, endTime, tagFilters);
                result.addAll(memoryResult);

                // 如果内存中有完整数据，直接返回
                if (ts.hasCompleteDataInRange(startTime, endTime)) {
                    logQueryPerformance(series, startTime, endTime, queryStart, result.size());
                    return result;
                }
            }

            // 从磁盘查询剩余数据
            List<DataPoint> diskResult = queryFromDisk(series, startTime, endTime, tagFilters);
            result.addAll(diskResult);

            // 按时间戳排序
            result.sort(Comparator.comparingLong(DataPoint::getTimestamp));

            logQueryPerformance(series, startTime, endTime, queryStart, result.size());
            return result;

        } finally {
            globalLock.readLock().unlock();
        }
    }

    public List<DataPoint> queryMultipleSeries(List<String> seriesList, long startTime, long endTime,
                                               Map<String, String> tagFilters) {
        List<DataPoint> allResults = new ArrayList<>();

        for (String series : seriesList) {
            List<DataPoint> seriesResults = query(series, startTime, endTime, tagFilters);
            allResults.addAll(seriesResults);
        }

        // 按时间戳排序
        allResults.sort(Comparator.comparingLong(DataPoint::getTimestamp));

        return allResults;
    }

    private List<DataPoint> queryFromDisk(String series, long startTime, long endTime,
                                          Map<String, String> tagFilters) {
        List<DataPoint> results = new ArrayList<>();

        try {
            // 获取序列元数据
            MetaFile.SeriesMeta seriesMeta = metaFile.getSeriesMeta(series);
            if (seriesMeta == null) {
                return results;
            }

            // 查询所有相关的数据文件
            List<MetaFile.DataFileInfo> relevantFiles = seriesMeta.getDataFiles().stream()
                    .filter(file -> !(endTime < file.getStartTime() || startTime > file.getEndTime()))
                    .sorted(Comparator.comparingLong(MetaFile.DataFileInfo::getStartTime))
                    .collect(Collectors.toList());

            // 并行查询多个文件
            List<Future<List<DataPoint>>> futures = new ArrayList<>();
            for (MetaFile.DataFileInfo fileInfo : relevantFiles) {
                futures.add(flushExecutor.submit(() ->
                        readDataPointsFromFile(series, fileInfo, startTime, endTime, tagFilters)));
            }

            // 收集结果
            for (Future<List<DataPoint>> future : futures) {
                try {
                    results.addAll(future.get(10, TimeUnit.SECONDS)); // 10秒超时
                } catch (Exception e) {
                    logger.warn("Error reading data file", e);
                }
            }

        } catch (Exception e) {
            logger.error("Error querying from disk for series: {}", series, e);
        }

        return results;
    }

    private List<DataPoint> readDataPointsFromFile(String series, MetaFile.DataFileInfo fileInfo,
                                                   long startTime, long endTime,
                                                   Map<String, String> tagFilters) {
        List<DataPoint> results = new ArrayList<>();

        try {
            // 读取索引文件获取数据位置
            IndexFile indexFile = new IndexFile(dataDir, series, fileInfo.getStartTime());
            List<IndexFile.IndexEntry> indexEntries = indexFile.findInRange(startTime, endTime);

            // 读取数据文件
            DataFile dataFile = new DataFile(dataDir, series, fileInfo.getStartTime());

            for (IndexFile.IndexEntry entry : indexEntries) {
                byte[] data = dataFile.readData(entry.fileOffset, entry.dataLength, true);
                if (data != null) {
                    DataPoint point = deserializeDataPoint(entry.timestamp, data);
                    if (point != null && matchesTags(point.getTags(), tagFilters)) {
                        results.add(point);
                    }
                }
            }

            indexFile.close();
            dataFile.close();

        } catch (Exception e) {
            logger.error("Error reading data points from file: {}", fileInfo.getFileName(), e);
        }

        return results;
    }

    private DataPoint deserializeDataPoint(long timestamp, byte[] data) {
        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(data);
            DataInputStream dis = new DataInputStream(bais);

            double value = dis.readDouble();
            int tagCount = dis.readInt();
            Map<String, String> tags = new HashMap<>();

            for (int i = 0; i < tagCount; i++) {
                String key = dis.readUTF();
                String valueStr = dis.readUTF();
                tags.put(key, valueStr);
            }

            return new DataPoint(timestamp, value, tags);

        } catch (Exception e) {
            logger.error("Error deserializing data point", e);
            return null;
        }
    }

    private boolean matchesTags(Map<String, String> pointTags, Map<String, String> filterTags) {
        if (filterTags == null || filterTags.isEmpty()) {
            return true;
        }

        for (Map.Entry<String, String> filter : filterTags.entrySet()) {
            String pointValue = pointTags.get(filter.getKey());
            if (pointValue == null || !pointValue.equals(filter.getValue())) {
                return false;
            }
        }

        return true;
    }

    /**
     * 刷写脏数据到磁盘
     */
    private void flushDirtySeries() {
        globalLock.readLock().lock();
        long startTime = System.currentTimeMillis();
        int flushedCount = 0;

        try {
            List<Future<Void>> futures = new ArrayList<>();

            for (Map.Entry<String, TimeSeries> entry : seriesCache.entrySet()) {
                TimeSeries ts = entry.getValue();

                if (ts.isDirty() && ts.getMemoryPointCount() > 0) {
                    futures.add(flushExecutor.submit(() -> {
                        try {
                            ts.flushToDisk();
                            totalFlushes.incrementAndGet();
                            return null;
                        } catch (Exception e) {
                            logger.error("Error flushing series: {}", entry.getKey(), e);
                            throw new RuntimeException(e);
                        }
                    }));
                }
            }

            // 等待所有刷写完成
            for (Future<Void> future : futures) {
                try {
                    future.get(30, TimeUnit.SECONDS); // 30秒超时
                    flushedCount++;
                } catch (TimeoutException e) {
                    logger.warn("Flush operation timed out");
                } catch (Exception e) {
                    logger.error("Error waiting for flush completion", e);
                }
            }

        } finally {
            globalLock.readLock().unlock();
        }

        long duration = System.currentTimeMillis() - startTime;
        if (flushedCount > 0) {
            logger.info("Flushed {} series to disk in {}ms", flushedCount, duration);
        }
    }

    /**
     * 刷写最老的时间序列
     */
    private void flushOldestSeries() {
        globalLock.readLock().lock();

        try {
            if (seriesCache.isEmpty()) {
                return;
            }

            // 找到内存中数据点最多的序列
            String oldestSeries = null;
            long maxPoints = 0;

            for (Map.Entry<String, TimeSeries> entry : seriesCache.entrySet()) {
                long pointCount = entry.getValue().getMemoryPointCount();
                if (pointCount > maxPoints) {
                    maxPoints = pointCount;
                    oldestSeries = entry.getKey();
                }
            }

            if (oldestSeries != null && maxPoints > 0) {
                TimeSeries ts = seriesCache.get(oldestSeries);
                if (ts != null) {
                    long startTime = System.currentTimeMillis();
                    ts.flushToDisk();
                    totalFlushes.incrementAndGet();

                    long duration = System.currentTimeMillis() - startTime;
                    logger.debug("Flushed oldest series {} ({} points) in {}ms",
                            oldestSeries, maxPoints, duration);
                }
            }

        } finally {
            globalLock.readLock().unlock();
        }
    }

    /**
     * 压缩旧文件
     */
    private void compactOldFiles() {
        globalLock.writeLock().lock();
        long startTime = System.currentTimeMillis();
        int compactedCount = 0;

        try {
            List<String> allSeries = metaFile.getAllSeries();
            List<Future<Integer>> futures = new ArrayList<>();

            for (String series : allSeries) {
                futures.add(flushExecutor.submit(() -> compactSeriesFiles(series)));
            }

            // 收集结果
            for (Future<Integer> future : futures) {
                try {
                    Integer count = future.get(60, TimeUnit.SECONDS); // 60秒超时
                    if (count != null) {
                        compactedCount += count;
                    }
                } catch (Exception e) {
                    logger.warn("Compaction operation failed or timed out", e);
                }
            }

            totalCompactions.incrementAndGet();

        } finally {
            globalLock.writeLock().unlock();
        }

        long duration = System.currentTimeMillis() - startTime;
        if (compactedCount > 0) {
            logger.info("Compacted {} files in {}ms", compactedCount, duration);
        }
    }

    private Integer compactSeriesFiles(String series) {
        int compactedFiles = 0;

        try {
            MetaFile.SeriesMeta seriesMeta = metaFile.getSeriesMeta(series);
            if (seriesMeta == null || seriesMeta.getDataFiles().size() < 2) {
                return 0;
            }

            List<MetaFile.DataFileInfo> dataFiles = seriesMeta.getDataFiles();
            dataFiles.sort(Comparator.comparingLong(MetaFile.DataFileInfo::getStartTime));

            // 合并小文件（小于10MB）
            List<MetaFile.DataFileInfo> smallFiles = dataFiles.stream()
                    .filter(f -> f.getFileSize() < 10 * 1024 * 1024) // 10MB
                    .collect(Collectors.toList());

            if (smallFiles.size() >= 2) {
                // 合并连续的小文件
                for (int i = 0; i < smallFiles.size() - 1; i++) {
                    MetaFile.DataFileInfo file1 = smallFiles.get(i);
                    MetaFile.DataFileInfo file2 = smallFiles.get(i + 1);

                    // 如果两个文件时间上连续，合并它们
                    if (file1.getEndTime() >= file2.getStartTime() - 3600000) { // 1小时间隔内
                        if (mergeDataFiles(series, file1, file2)) {
                            compactedFiles++;
                            i++; // 跳过下一个文件，因为它已经被合并
                        }
                    }
                }
            }

            // 更新元数据
            if (compactedFiles > 0) {
                metaFile.saveSeriesMeta(series, seriesMeta);
            }

        } catch (Exception e) {
            logger.error("Error compacting files for series: {}", series, e);
        }

        return compactedFiles;
    }

    private boolean mergeDataFiles(String series, MetaFile.DataFileInfo file1, MetaFile.DataFileInfo file2) {
        try {
            String tempFilePath = Paths.get(dataDir, "temp",
                    series + "_merge_" + System.currentTimeMillis() + ".dat").toString();

            // 创建新合并文件
            DataFile mergedFile = new DataFile(dataDir, series, System.currentTimeMillis());

            // 读取并合并两个文件的数据
            DataFile df1 = new DataFile(dataDir, series, file1.getStartTime());
            DataFile df2 = new DataFile(dataDir, series, file2.getStartTime());

            // TODO: 实现实际的数据合并逻辑

            df1.close();
            df2.close();
            mergedFile.close();

            // 删除旧文件
            Path file1Path = Paths.get(dataDir, "data", file1.getFileName());
            Path file2Path = Paths.get(dataDir, "data", file2.getFileName());
            Files.deleteIfExists(file1Path);
            Files.deleteIfExists(file2Path);

            // 删除旧索引文件
            Path idx1Path = Paths.get(dataDir, "index",
                    file1.getFileName().replace(".dat", ".idx"));
            Path idx2Path = Paths.get(dataDir, "index",
                    file2.getFileName().replace(".dat", ".idx"));
            Files.deleteIfExists(idx1Path);
            Files.deleteIfExists(idx2Path);

            logger.debug("Merged files {} and {} for series {}",
                    file1.getFileName(), file2.getFileName(), series);

            return true;

        } catch (Exception e) {
            logger.error("Error merging files", e);
            return false;
        }
    }

    /**
     * 清理过期数据
     */
    private void cleanupExpiredData(int retentionDays) {
        if (retentionDays <= 0) {
            return; // 永不过期
        }

        long cutoffTime = System.currentTimeMillis() - (retentionDays * 24 * 60 * 60 * 1000L);
        int deletedCount = 0;

        try {
            List<String> allSeries = metaFile.getAllSeries();

            for (String series : allSeries) {
                MetaFile.SeriesMeta seriesMeta = metaFile.getSeriesMeta(series);
                if (seriesMeta == null) {
                    continue;
                }

                List<MetaFile.DataFileInfo> oldFiles = seriesMeta.getDataFiles().stream()
                        .filter(file -> file.getEndTime() < cutoffTime)
                        .collect(Collectors.toList());

                for (MetaFile.DataFileInfo oldFile : oldFiles) {
                    // 删除数据文件
                    Path dataPath = Paths.get(dataDir, "data", oldFile.getFileName());
                    Files.deleteIfExists(dataPath);

                    // 删除索引文件
                    Path indexPath = Paths.get(dataDir, "index",
                            oldFile.getFileName().replace(".dat", ".idx"));
                    Files.deleteIfExists(indexPath);

                    seriesMeta.getDataFiles().remove(oldFile);
                    deletedCount++;
                }

                // 保存更新后的元数据
                if (!oldFiles.isEmpty()) {
                    metaFile.saveSeriesMeta(series, seriesMeta);
                }
            }

            // 清理临时目录
            cleanTempDirectory();

        } catch (Exception e) {
            logger.error("Error cleaning up expired data", e);
        }

        if (deletedCount > 0) {
            logger.info("Cleaned up {} expired data files (older than {} days)",
                    deletedCount, retentionDays);
        }
    }

    private void cleanTempDirectory() {
        Path tempDir = Paths.get(dataDir, "temp");
        if (!Files.exists(tempDir)) {
            return;
        }

        try {
            Files.walk(tempDir)
                    .filter(Files::isRegularFile)
                    .filter(path -> {
                        try {
                            return Files.getLastModifiedTime(path).toMillis() <
                                    System.currentTimeMillis() - 3600000; // 1小时前
                        } catch (IOException e) {
                            return false;
                        }
                    })
                    .forEach(path -> {
                        try {
                            Files.delete(path);
                        } catch (IOException e) {
                            logger.warn("Failed to delete temp file: {}", path, e);
                        }
                    });
        } catch (IOException e) {
            logger.warn("Error cleaning temp directory", e);
        }
    }

    /**
     * 获取所有序列名称
     */
    public List<String> getAllSeries() {
        try {
            return metaFile.getAllSeries();
        } catch (Exception e) {
            logger.error("Error getting all series", e);
            return new ArrayList<>();
        }
    }

    /**
     * 获取序列统计信息
     */
    public Map<String, Object> getSeriesStats(String series) {
        Map<String, Object> stats = new HashMap<>();

        try {
            MetaFile.SeriesMeta meta = metaFile.getSeriesMeta(series);
            if (meta != null) {
                stats.put("name", meta.getName());
                stats.put("createdTime", meta.getCreatedTime());
                stats.put("lastUpdated", meta.getLastUpdated());
                stats.put("totalPoints", meta.getTotalPoints());
                stats.put("dataFileCount", meta.getDataFiles().size());
                stats.put("tags", meta.getTags());

                long totalSize = meta.getDataFiles().stream()
                        .mapToLong(MetaFile.DataFileInfo::getFileSize)
                        .sum();
                stats.put("totalSizeBytes", totalSize);
            }

            // 添加内存中的统计
            TimeSeries ts = seriesCache.get(series);
            if (ts != null) {
                stats.put("memoryPoints", ts.getMemoryPointCount());
                stats.put("dirty", ts.isDirty());
            }

        } catch (Exception e) {
            logger.error("Error getting stats for series: {}", series, e);
        }

        return stats;
    }

    /**
     * 获取数据库统计信息
     */
    public Map<String, Object> getDatabaseStats() {
        Map<String, Object> stats = new HashMap<>();

        stats.put("totalSeries", seriesCache.size());
        stats.put("totalWrites", totalWrites.get());
        stats.put("totalQueries", totalQueries.get());
        stats.put("totalFlushes", totalFlushes.get());
        stats.put("totalCompactions", totalCompactions.get());
        stats.put("totalMemoryPoints", getTotalMemoryPoints());
        stats.put("maxMemoryPoints", maxMemoryPoints);
        stats.put("memoryUsagePercent", (getTotalMemoryPoints() * 100.0) / maxMemoryPoints);

        // 磁盘使用情况
        try {
            long diskUsage = calculateDiskUsage();
            stats.put("diskUsageBytes", diskUsage);
        } catch (Exception e) {
            logger.warn("Error calculating disk usage", e);
        }

        return stats;
    }

    private long calculateDiskUsage() throws IOException {
        return Files.walk(Paths.get(dataDir))
                .filter(Files::isRegularFile)
                .mapToLong(path -> {
                    try {
                        return Files.size(path);
                    } catch (IOException e) {
                        return 0L;
                    }
                })
                .sum();
    }

    private long getTotalMemoryPoints() {
        return seriesCache.values().stream()
                .mapToLong(TimeSeries::getMemoryPointCount)
                .sum();
    }

    private void logQueryPerformance(String series, long startTime, long endTime,
                                     long queryStart, int resultCount) {
        long duration = System.currentTimeMillis() - queryStart;

        if (duration > 1000) { // 慢查询日志
            logger.warn("Slow query for series {}: {} results in {}ms (range: {} to {})",
                    series, resultCount, duration,
                    new Date(startTime), new Date(endTime));
        } else if (logger.isDebugEnabled()) {
            logger.debug("Query for series {}: {} results in {}ms",
                    series, resultCount, duration);
        }
    }

    private void logStatistics() {
        Map<String, Object> stats = getDatabaseStats();
        logger.info("Database statistics: {}", stats);

        // 保存统计信息到文件
        try {
            MetaFile.DatabaseStats dbStats = metaFile.loadStats();
            dbStats.update("statistics", 0, 0); // 更新统计
            metaFile.saveStats(dbStats);
        } catch (IOException e) {
            logger.warn("Failed to save statistics", e);
        }
    }

    public void close() {
        logger.info("Shutting down TimeSeriesDB...");

        // 停止后台任务
        compactionExecutor.shutdown();
        cleanupExecutor.shutdown();
        flushExecutor.shutdown();

        try {
            // 等待任务完成
            if (!compactionExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                compactionExecutor.shutdownNow();
            }

            if (!cleanupExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                cleanupExecutor.shutdownNow();
            }

            // 刷写所有数据
            flushAllSeries();

            // 等待刷写完成
            if (!flushExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
                flushExecutor.shutdownNow();
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("Shutdown interrupted", e);
            flushExecutor.shutdownNow();
            compactionExecutor.shutdownNow();
            cleanupExecutor.shutdownNow();
        }

        logger.info("TimeSeriesDB shutdown completed");
    }

    private void flushAllSeries() {
        globalLock.writeLock().lock();
        long startTime = System.currentTimeMillis();

        try {
            logger.info("Flushing all series to disk...");

            List<Future<Void>> futures = new ArrayList<>();
            for (Map.Entry<String, TimeSeries> entry : seriesCache.entrySet()) {
                futures.add(flushExecutor.submit(() -> {
                    try {
                        entry.getValue().flushToDisk();
                        return null;
                    } catch (Exception e) {
                        logger.error("Error flushing series during shutdown: {}", entry.getKey(), e);
                        throw new RuntimeException(e);
                    }
                }));
            }

            // 等待所有刷写完成
            for (Future<Void> future : futures) {
                try {
                    future.get(30, TimeUnit.SECONDS);
                } catch (Exception e) {
                    logger.warn("Flush operation failed during shutdown", e);
                }
            }

            seriesCache.clear();

            long duration = System.currentTimeMillis() - startTime;
            logger.info("All series flushed to disk in {}ms", duration);

        } finally {
            globalLock.writeLock().unlock();
        }
    }

    /**
     * 备份数据库
     */
    public void backup(String backupPath) throws IOException {
        globalLock.readLock().lock();

        try {
            logger.info("Starting database backup to: {}", backupPath);

            // 先刷写所有数据
            flushAllSeries();

            // 复制整个数据目录
            Path sourceDir = Paths.get(dataDir);
            Path targetDir = Paths.get(backupPath);

            Files.createDirectories(targetDir);

            // 使用 Files.walk 复制文件
            Files.walk(sourceDir)
                    .forEach(source -> {
                        Path target = targetDir.resolve(sourceDir.relativize(source));
                        try {
                            if (Files.isDirectory(source)) {
                                Files.createDirectories(target);
                            } else {
                                Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING);
                            }
                        } catch (IOException e) {
                            throw new RuntimeException("Backup failed", e);
                        }
                    });

            logger.info("Database backup completed to: {}", backupPath);

        } finally {
            globalLock.readLock().unlock();
        }
    }

    /**
     * 恢复数据库
     */
    public void restore(String backupPath) throws IOException {
        globalLock.writeLock().lock();

        try {
            logger.info("Starting database restore from: {}", backupPath);

            // 关闭当前数据库
            close();

            // 清空当前数据目录
            Files.walk(Paths.get(dataDir))
                    .sorted(Comparator.reverseOrder())
                    .forEach(path -> {
                        try {
                            Files.deleteIfExists(path);
                        } catch (IOException e) {
                            logger.warn("Failed to delete: {}", path, e);
                        }
                    });

            // 从备份恢复
            Path backupDir = Paths.get(backupPath);
            Path targetDir = Paths.get(dataDir);

            Files.walk(backupDir)
                    .forEach(source -> {
                        Path target = targetDir.resolve(backupDir.relativize(source));
                        try {
                            if (Files.isDirectory(source)) {
                                Files.createDirectories(target);
                            } else {
                                Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING);
                            }
                        } catch (IOException e) {
                            throw new RuntimeException("Restore failed", e);
                        }
                    });

            logger.info("Database restore completed from: {}", backupPath);

        } finally {
            globalLock.writeLock().unlock();
        }
    }
}