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
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.zip.*;

public class TimeSeries {
    private static final Logger logger = LoggerFactory.getLogger(TimeSeries.class);

    private final String name;
    private final String dataDir;
    private final boolean enableCompression;
    private final long maxMemoryPoints;
    private final MetaFile metaFile;

    // 内存中的数据结构
    private final ConcurrentSkipListMap<Long, DataPoint> inMemoryPoints;
    private final Map<String, TagIndex> tagIndexes;
    private final ReentrantReadWriteLock lock;
    private final AtomicInteger pointCount;

    // 脏数据标志
    private volatile boolean dirty = false;
    private volatile long lastFlushTime = System.currentTimeMillis();
    private volatile long lastWriteTime = System.currentTimeMillis();

    // 统计信息
    private final AtomicLong totalPointsWritten = new AtomicLong(0);
    private final AtomicLong totalPointsFlushed = new AtomicLong(0);
    private final AtomicLong totalQueries = new AtomicLong(0);

    // 文件存储相关
    private RandomAccessFile currentDataFile;
    private FileChannel currentDataChannel;
    private long currentFileOffset;
    private String currentFileName;
    private long currentFileStartTime;

    // WAL (Write-Ahead Log) 相关
    private RandomAccessFile walFile;
    private FileChannel walChannel;
    private boolean enableWAL = true;

    // 配置常量
    private static final int FILE_BLOCK_SIZE = 4 * 1024 * 1024; // 4MB
    private static final int WAL_FLUSH_INTERVAL = 1000; // 1秒刷写一次WAL
    private static final int MAX_WAL_SIZE = 10 * 1024 * 1024; // 10MB

    // 序列元数据
    private MetaFile.SeriesMeta seriesMeta;

    public TimeSeries(String name, String dataDir, boolean enableCompression, long maxMemoryPoints) {
        this.name = name;
        this.dataDir = dataDir;
        this.enableCompression = enableCompression;
        this.maxMemoryPoints = maxMemoryPoints;
        this.metaFile = new MetaFile(dataDir);

        this.inMemoryPoints = new ConcurrentSkipListMap<>();
        this.tagIndexes = new HashMap<>();
        this.lock = new ReentrantReadWriteLock();
        this.pointCount = new AtomicInteger(0);

        // 加载或创建序列元数据
        loadOrCreateSeriesMeta();

        // 打开当前数据文件
        openCurrentFile();

        // 如果需要，打开WAL文件
        if (enableWAL) {
            openWALFile();
        }

        logger.debug("TimeSeries initialized: {}, maxMemoryPoints: {}", name, maxMemoryPoints);
    }

    /**
     * 加载或创建序列元数据
     */
    private void loadOrCreateSeriesMeta() {
        try {
            seriesMeta = metaFile.getSeriesMeta(name);
            if (seriesMeta == null) {
                seriesMeta = new MetaFile.SeriesMeta(name);
                metaFile.saveSeriesMeta(name, seriesMeta);
                logger.info("Created new series: {}", name);
            } else {
                logger.debug("Loaded existing series: {}, total points: {}",
                        name, seriesMeta.getTotalPoints());
            }
        } catch (IOException e) {
            logger.error("Failed to load/create series meta for: {}", name, e);
            // 创建内存中的元数据作为后备
            seriesMeta = new MetaFile.SeriesMeta(name);
        }
    }

    /**
     * 打开当前数据文件
     */
    private void openCurrentFile() {
        try {
            currentFileStartTime = System.currentTimeMillis();
            String fileName = String.format("%s_%d.dat", name, currentFileStartTime);
            currentFileName = Paths.get(dataDir, "data", fileName).toString();

            // 确保目录存在
            Path parentDir = Paths.get(dataDir, "data");
            if (!Files.exists(parentDir)) {
                Files.createDirectories(parentDir);
            }

            currentDataFile = new RandomAccessFile(currentFileName, "rw");
            currentDataChannel = currentDataFile.getChannel();
            currentFileOffset = 0;

            // 写入文件头
            writeFileHeader();

            logger.debug("Opened data file: {}", currentFileName);
        } catch (IOException e) {
            logger.error("Failed to open data file for series: {}", name, e);
            throw new RuntimeException("Failed to open data file", e);
        }
    }

    /**
     * 写入文件头
     */
    private void writeFileHeader() throws IOException {
        ByteBuffer headerBuffer = ByteBuffer.allocate(64); // 64字节头

        // 魔数
        headerBuffer.put("TSDB".getBytes());

        // 版本
        headerBuffer.putInt(1);

        // 序列名称长度和内容
        byte[] nameBytes = name.getBytes("UTF-8");
        headerBuffer.putInt(nameBytes.length);
        headerBuffer.put(nameBytes);

        // 时间范围（初始为0）
        headerBuffer.putLong(0L); // 最小时间戳
        headerBuffer.putLong(0L); // 最大时间戳

        // 数据点数量
        headerBuffer.putInt(0);

        // 压缩标志
        headerBuffer.put(enableCompression ? (byte)1 : (byte)0);

        // 填充剩余空间
        while (headerBuffer.position() < 64) {
            headerBuffer.put((byte)0);
        }

        headerBuffer.flip();
        currentDataChannel.write(headerBuffer, 0);
        currentFileOffset = 64;
    }

    /**
     * 打开WAL文件
     */
    private void openWALFile() {
        try {
            String walFileName = String.format("%s_%d.wal", name, System.currentTimeMillis());
            String walPath = Paths.get(dataDir, "wal", walFileName).toString();

            // 确保WAL目录存在
            Path walDir = Paths.get(dataDir, "wal");
            if (!Files.exists(walDir)) {
                Files.createDirectories(walDir);
            }

            walFile = new RandomAccessFile(walPath, "rw");
            walChannel = walFile.getChannel();

            logger.debug("Opened WAL file: {}", walPath);
        } catch (IOException e) {
            logger.error("Failed to open WAL file for series: {}", name, e);
            enableWAL = false; // 禁用WAL功能
        }
    }

    /**
     * 写入数据点
     */
    public void writePoint(long timestamp, double value, Map<String, String> tags) {
        lock.writeLock().lock();
        long startTime = System.currentTimeMillis();

        try {
            DataPoint point = new DataPoint(timestamp, value, tags);

            // 写入内存
            inMemoryPoints.put(timestamp, point);
            updateTagIndexes(timestamp, tags);

            // 更新统计
            int currentCount = pointCount.incrementAndGet();
            totalPointsWritten.incrementAndGet();
            dirty = true;
            lastWriteTime = System.currentTimeMillis();

            // 写入WAL（如果启用）
            if (enableWAL) {
                writeToWAL(timestamp, value, tags);
            }

            // 检查是否需要刷写
            if (currentCount >= maxMemoryPoints) {
                flushToDisk();
            }

            // 记录性能
            long duration = System.currentTimeMillis() - startTime;
            if (duration > 50) { // 慢写入日志
                logger.warn("Slow write to series {}: {}ms", name, duration);
            }

        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 批量写入数据点
     */
    public void writePointsBatch(List<DataPoint> points) {
        if (points == null || points.isEmpty()) {
            return;
        }

        lock.writeLock().lock();
        long startTime = System.currentTimeMillis();

        try {
            for (DataPoint point : points) {
                inMemoryPoints.put(point.getTimestamp(), point);
                updateTagIndexes(point.getTimestamp(), point.getTags());
            }

            int addedCount = points.size();
            pointCount.addAndGet(addedCount);
            totalPointsWritten.addAndGet(addedCount);
            dirty = true;
            lastWriteTime = System.currentTimeMillis();

            // 批量写入WAL
            if (enableWAL) {
                for (DataPoint point : points) {
                    writeToWAL(point.getTimestamp(), point.getValue(), point.getTags());
                }
            }

            // 检查是否需要刷写
            if (pointCount.get() >= maxMemoryPoints) {
                flushToDisk();
            }

            long duration = System.currentTimeMillis() - startTime;
            logger.debug("Batch write to series {}: {} points in {}ms",
                    name, addedCount, duration);

        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 写入WAL
     */
    private void writeToWAL(long timestamp, double value, Map<String, String> tags) {
        if (walChannel == null || !walChannel.isOpen()) {
            return;
        }

        try {
            byte[] walData = serializeForWAL(timestamp, value, tags);
            ByteBuffer buffer = ByteBuffer.allocate(walData.length + 4);
            buffer.putInt(walData.length);
            buffer.put(walData);
            buffer.flip();

            walChannel.write(buffer);

            // 定期刷写WAL
            if (System.currentTimeMillis() - lastFlushTime > WAL_FLUSH_INTERVAL) {
                walChannel.force(true);
                lastFlushTime = System.currentTimeMillis();
            }

            // 检查WAL文件大小
            if (walChannel.size() > MAX_WAL_SIZE) {
                rotateWALFile();
            }

        } catch (IOException e) {
            logger.error("Failed to write to WAL for series: {}", name, e);
        }
    }

    /**
     * 序列化用于WAL的数据
     */
    private byte[] serializeForWAL(long timestamp, double value, Map<String, String> tags) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(baos)) {

            dos.writeLong(timestamp);
            dos.writeDouble(value);

            dos.writeInt(tags.size());
            for (Map.Entry<String, String> entry : tags.entrySet()) {
                dos.writeUTF(entry.getKey());
                dos.writeUTF(entry.getValue());
            }

            return baos.toByteArray();

        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize for WAL", e);
        }
    }

    /**
     * 轮换WAL文件
     */
    private void rotateWALFile() {
        try {
            if (walChannel != null && walChannel.isOpen()) {
                walChannel.force(true);
                walChannel.close();
            }
            if (walFile != null) {
                walFile.close();
            }

            // 打开新的WAL文件
            openWALFile();

            logger.debug("Rotated WAL file for series: {}", name);

        } catch (IOException e) {
            logger.error("Failed to rotate WAL file for series: {}", name, e);
        }
    }

    /**
     * 更新标签索引
     */
    private void updateTagIndexes(long timestamp, Map<String, String> tags) {
        for (Map.Entry<String, String> entry : tags.entrySet()) {
            String tagKey = entry.getKey();
            String tagValue = entry.getValue();

            TagIndex index = tagIndexes.computeIfAbsent(tagKey,
                    k -> new TagIndex(tagKey));
            index.addIndex(tagValue, timestamp);
        }
    }

    /**
     * 刷写到磁盘
     */
    public void flushToDisk() {
        lock.writeLock().lock();
        long startTime = System.currentTimeMillis();

        try {
            if (inMemoryPoints.isEmpty() || !dirty) {
                return;
            }

            logger.debug("Flushing series {} to disk ({} points)",
                    name, pointCount.get());

            // 写入数据文件
            List<Long> timestamps = new ArrayList<>();
            long minTimestamp = Long.MAX_VALUE;
            long maxTimestamp = Long.MIN_VALUE;
            int pointsFlushed = 0;

            ByteBuffer buffer = ByteBuffer.allocate(FILE_BLOCK_SIZE);

            for (Map.Entry<Long, DataPoint> entry : inMemoryPoints.entrySet()) {
                long timestamp = entry.getKey();
                DataPoint point = entry.getValue();

                // 更新时间范围
                if (timestamp < minTimestamp) minTimestamp = timestamp;
                if (timestamp > maxTimestamp) maxTimestamp = timestamp;

                // 序列化数据点
                byte[] serialized = serializeDataPoint(point);

                // 检查缓冲区空间
                if (buffer.remaining() < serialized.length + 12) { // 时间戳(8) + 长度(4)
                    writeBufferToFile(buffer);
                    buffer.clear();
                }

                // 写入时间戳和数据
                buffer.putLong(timestamp);
                buffer.putInt(serialized.length);
                buffer.put(serialized);

                timestamps.add(timestamp);
                pointsFlushed++;

                // 更新写入计数
                totalPointsFlushed.incrementAndGet();
            }

            // 写入剩余的缓冲区数据
            if (buffer.position() > 0) {
                writeBufferToFile(buffer);
            }

            // 更新文件头
            updateFileHeader(minTimestamp, maxTimestamp, pointsFlushed);

            // 写入索引文件
            if (!timestamps.isEmpty()) {
                writeIndexFile(timestamps, minTimestamp, maxTimestamp);
            }

            // 更新序列元数据
            updateSeriesMeta(minTimestamp, maxTimestamp, pointsFlushed);

            // 清空内存数据
            inMemoryPoints.clear();
            tagIndexes.clear();
            pointCount.set(0);
            dirty = false;

            // 关闭当前文件，打开新文件
            closeCurrentFile();
            openCurrentFile();

            // 清理WAL（数据已持久化）
            clearWAL();

            long duration = System.currentTimeMillis() - startTime;
            logger.info("Flushed series {}: {} points in {}ms",
                    name, pointsFlushed, duration);

        } catch (IOException e) {
            logger.error("Failed to flush series {} to disk", name, e);
            throw new RuntimeException("Failed to flush to disk", e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 更新文件头
     */
    private void updateFileHeader(long minTimestamp, long maxTimestamp, int pointCount)
            throws IOException {
        if (minTimestamp == Long.MAX_VALUE || maxTimestamp == Long.MIN_VALUE) {
            return;
        }

        ByteBuffer headerBuffer = ByteBuffer.allocate(24); // 时间戳范围 + 计数

        headerBuffer.putLong(minTimestamp);
        headerBuffer.putLong(maxTimestamp);
        headerBuffer.putInt(pointCount);

        headerBuffer.flip();
        currentDataChannel.write(headerBuffer, 32); // 跳过前32字节（魔数、版本、名称）
    }

    /**
     * 序列化数据点
     */
    private byte[] serializeDataPoint(DataPoint point) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(baos)) {

            dos.writeDouble(point.getValue());

            Map<String, String> tags = point.getTags();
            dos.writeInt(tags.size());

            for (Map.Entry<String, String> entry : tags.entrySet()) {
                dos.writeUTF(entry.getKey());
                dos.writeUTF(entry.getValue());
            }

            byte[] data = baos.toByteArray();

            if (enableCompression) {
                return compress(data);
            }

            return data;
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize data point", e);
        }
    }

    /**
     * 压缩数据
     */
    private byte[] compress(byte[] data) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DeflaterOutputStream dos = new DeflaterOutputStream(baos)) {
            dos.write(data);
            dos.finish();
            return baos.toByteArray();
        } catch (IOException e) {
            logger.warn("Compression failed for series {}, using uncompressed data", name, e);
            return data; // 压缩失败，返回原始数据
        }
    }

    /**
     * 解压数据
     */
    private byte[] decompress(byte[] compressed) {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(compressed);
             InflaterInputStream iis = new InflaterInputStream(bais);
             ByteArrayOutputStream baos = new ByteArrayOutputStream()) {

            byte[] buffer = new byte[1024];
            int len;
            while ((len = iis.read(buffer)) > 0) {
                baos.write(buffer, 0, len);
            }
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Decompression failed", e);
        }
    }

    /**
     * 写入缓冲区到文件
     */
    private void writeBufferToFile(ByteBuffer buffer) throws IOException {
        buffer.flip();
        currentDataChannel.write(buffer);
        currentFileOffset += buffer.limit();
    }

    /**
     * 写入索引文件
     */
    private void writeIndexFile(List<Long> timestamps, long minTime, long maxTime)
            throws IOException {
        String indexPath = Paths.get(dataDir, "index",
                String.format("%s_%d.idx", name, currentFileStartTime)).toString();

        // 确保索引目录存在
        Path indexDir = Paths.get(dataDir, "index");
        if (!Files.exists(indexDir)) {
            Files.createDirectories(indexDir);
        }

        try (RandomAccessFile idxFile = new RandomAccessFile(indexPath, "rw");
             FileChannel idxChannel = idxFile.getChannel()) {

            // 使用更大的缓冲区提高性能
            int bufferSize = Math.min(8 * 1024 * 1024, timestamps.size() * 20 + 100);
            ByteBuffer idxBuffer = ByteBuffer.allocate(bufferSize);

            // 写入时间范围
            idxBuffer.putLong(minTime);
            idxBuffer.putLong(maxTime);

            // 写入时间戳数量
            idxBuffer.putInt(timestamps.size());

            // 写入每个时间戳和对应的文件偏移量
            // 注意：这里简化了偏移量计算，实际应该记录每个数据点的精确偏移
            long baseOffset = 64; // 文件头大小
            for (Long ts : timestamps) {
                idxBuffer.putLong(ts);
                idxBuffer.putLong(baseOffset);
                // 估算每个数据点的偏移量（时间戳8 + 长度4 + 数据长度）
                baseOffset += 12 + estimateDataPointSize();
            }

            idxBuffer.flip();
            idxChannel.write(idxBuffer);

            logger.debug("Created index file: {} ({} entries)", indexPath, timestamps.size());
        }
    }

    /**
     * 估算数据点大小
     */
    private int estimateDataPointSize() {
        // 简化估算：值(8) + 标签数量(4) + 每个标签(平均20字节)
        return 8 + 4 + (20 * 3); // 假设平均3个标签
    }

    /**
     * 更新序列元数据
     */
    private void updateSeriesMeta(long minTimestamp, long maxTimestamp, int pointsFlushed) {
        try {
            // 更新统计信息
            seriesMeta.setTotalPoints(seriesMeta.getTotalPoints() + pointsFlushed);
            seriesMeta.setLastUpdated(System.currentTimeMillis());

            // 添加数据文件信息
            MetaFile.DataFileInfo fileInfo = new MetaFile.DataFileInfo();
            fileInfo.setFileName(Paths.get(currentFileName).getFileName().toString());
            fileInfo.setStartTime(minTimestamp);
            fileInfo.setEndTime(maxTimestamp);
            fileInfo.setFileSize(currentFileOffset);
            fileInfo.setPointCount(pointsFlushed);
            fileInfo.setCompression(enableCompression ? "deflate" : "none");

            seriesMeta.addDataFile(fileInfo);

            // 保存到文件
            metaFile.saveSeriesMeta(name, seriesMeta);

        } catch (IOException e) {
            logger.error("Failed to update series meta for: {}", name, e);
        }
    }

    /**
     * 关闭当前文件
     */
    private void closeCurrentFile() {
        try {
            if (currentDataChannel != null && currentDataChannel.isOpen()) {
                currentDataChannel.force(true);
                currentDataChannel.close();
            }
            if (currentDataFile != null) {
                currentDataFile.close();
            }
        } catch (IOException e) {
            logger.warn("Failed to close data file for series: {}", name, e);
        }
    }

    /**
     * 清理WAL
     */
    private void clearWAL() {
        try {
            if (walChannel != null && walChannel.isOpen()) {
                walChannel.truncate(0);
                walChannel.force(true);
            }
        } catch (IOException e) {
            logger.warn("Failed to clear WAL for series: {}", name, e);
        }
    }

    /**
     * 从内存查询
     */
    public List<DataPoint> queryFromMemory(long startTime, long endTime,
                                           Map<String, String> tagFilters) {
        lock.readLock().lock();
        totalQueries.incrementAndGet();
        long start = System.currentTimeMillis();

        try {
            List<DataPoint> result = new ArrayList<>();

            NavigableMap<Long, DataPoint> subMap =
                    inMemoryPoints.subMap(startTime, true, endTime, true);

            for (DataPoint point : subMap.values()) {
                if (matchesTags(point.getTags(), tagFilters)) {
                    result.add(point);
                }
            }

            // 记录查询性能
            long duration = System.currentTimeMillis() - start;
            if (duration > 100 && !result.isEmpty()) {
                logger.debug("Slow memory query for series {}: {} results in {}ms",
                        name, result.size(), duration);
            }

            return result;

        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 标签匹配
     */
    private boolean matchesTags(Map<String, String> pointTags,
                                Map<String, String> filterTags) {
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
     * 检查指定范围内是否有完整数据
     */
    public boolean hasCompleteDataInRange(long startTime, long endTime) {
        lock.readLock().lock();
        try {
            if (inMemoryPoints.isEmpty()) {
                return false;
            }

            Long firstKey = inMemoryPoints.firstKey();
            Long lastKey = inMemoryPoints.lastKey();

            return firstKey <= startTime && lastKey >= endTime;

        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 内存中是否有数据
     */
    public boolean hasDataInMemory() {
        return !inMemoryPoints.isEmpty();
    }

    /**
     * 获取内存中的数据点数量
     */
    public int getMemoryPointCount() {
        return pointCount.get();
    }

    /**
     * 是否是脏数据
     */
    public boolean isDirty() {
        return dirty;
    }

    /**
     * 获取最后写入时间
     */
    public long getLastWriteTime() {
        return lastWriteTime;
    }

    /**
     * 获取最后刷写时间
     */
    public long getLastFlushTime() {
        return lastFlushTime;
    }

    /**
     * 获取序列统计信息
     */
    public Map<String, Object> getStats() {
        Map<String, Object> stats = new HashMap<>();

        stats.put("name", name);
        stats.put("memoryPoints", pointCount.get());
        stats.put("dirty", dirty);
        stats.put("lastWriteTime", lastWriteTime);
        stats.put("lastFlushTime", lastFlushTime);
        stats.put("totalPointsWritten", totalPointsWritten.get());
        stats.put("totalPointsFlushed", totalPointsFlushed.get());
        stats.put("totalQueries", totalQueries.get());
        stats.put("enableCompression", enableCompression);
        stats.put("enableWAL", enableWAL);

        if (seriesMeta != null) {
            stats.put("totalPointsOnDisk", seriesMeta.getTotalPoints());
            stats.put("dataFiles", seriesMeta.getDataFiles().size());
        }

        return stats;
    }

    /**
     * 关闭序列
     */
    public void close() {
        lock.writeLock().lock();

        try {
            // 刷写剩余数据
            if (dirty && !inMemoryPoints.isEmpty()) {
                logger.info("Closing series {}, flushing remaining {} points",
                        name, pointCount.get());
                flushToDisk();
            }

            // 关闭文件
            closeCurrentFile();

            if (walChannel != null && walChannel.isOpen()) {
                walChannel.close();
            }
            if (walFile != null) {
                walFile.close();
            }

            // 清理内存
            inMemoryPoints.clear();
            tagIndexes.clear();

            logger.info("Series {} closed", name);

        } catch (Exception e) {
            logger.error("Error closing series: {}", name, e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 恢复WAL数据（启动时调用）
     */
    public void recoverFromWAL() {
        if (!enableWAL) {
            return;
        }

        lock.writeLock().lock();

        try {
            // 查找最新的WAL文件
            Path walDir = Paths.get(dataDir, "wal");
            if (!Files.exists(walDir)) {
                return;
            }

            List<Path> walFiles = new ArrayList<>();
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(walDir,
                    path -> path.getFileName().toString().startsWith(name + "_") &&
                            path.getFileName().toString().endsWith(".wal"))) {
                for (Path path : stream) {
                    walFiles.add(path);
                }
            }

            // 按时间排序
            walFiles.sort(Comparator.comparing(path -> path.getFileName().toString()));

            // 恢复最新的WAL文件
            if (!walFiles.isEmpty()) {
                Path latestWal = walFiles.get(walFiles.size() - 1);
                recoverFromWalFile(latestWal);

                // 删除旧的WAL文件
                for (Path walFile : walFiles) {
                    if (!walFile.equals(latestWal)) {
                        Files.deleteIfExists(walFile);
                    }
                }
            }

        } catch (Exception e) {
            logger.error("Failed to recover from WAL for series: {}", name, e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 从WAL文件恢复数据
     */
    private void recoverFromWalFile(Path walPath) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(walPath.toFile(), "r");
             FileChannel channel = raf.getChannel()) {

            long fileSize = channel.size();
            long position = 0;
            int recoveredCount = 0;

            ByteBuffer sizeBuffer = ByteBuffer.allocate(4);

            while (position < fileSize) {
                // 读取数据长度
                sizeBuffer.clear();
                channel.read(sizeBuffer, position);
                sizeBuffer.flip();
                int dataSize = sizeBuffer.getInt();
                position += 4;

                if (dataSize <= 0 || dataSize > 1024 * 1024) { // 限制最大1MB
                    logger.warn("Invalid data size in WAL: {}", dataSize);
                    break;
                }

                // 读取数据
                ByteBuffer dataBuffer = ByteBuffer.allocate(dataSize);
                channel.read(dataBuffer, position);
                dataBuffer.flip();
                position += dataSize;

                // 反序列化数据点
                DataPoint point = deserializeFromWAL(dataBuffer.array());
                if (point != null) {
                    inMemoryPoints.put(point.getTimestamp(), point);
                    updateTagIndexes(point.getTimestamp(), point.getTags());
                    pointCount.incrementAndGet();
                    recoveredCount++;
                    dirty = true;
                }
            }

            if (recoveredCount > 0) {
                logger.info("Recovered {} points from WAL for series: {}",
                        recoveredCount, name);
            }

        } catch (Exception e) {
            throw new IOException("Failed to recover from WAL file: " + walPath, e);
        }
    }

    /**
     * 从WAL数据反序列化
     */
    private DataPoint deserializeFromWAL(byte[] data) {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(data);
             DataInputStream dis = new DataInputStream(bais)) {

            long timestamp = dis.readLong();
            double value = dis.readDouble();

            int tagCount = dis.readInt();
            Map<String, String> tags = new HashMap<>();
            for (int i = 0; i < tagCount; i++) {
                String key = dis.readUTF();
                String valueStr = dis.readUTF();
                tags.put(key, valueStr);
            }

            return new DataPoint(timestamp, value, tags);

        } catch (IOException e) {
            logger.error("Failed to deserialize from WAL", e);
            return null;
        }
    }
}