// storage/DataFile.java - 完善的数据文件管理
package com.timeseries.db.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

public class DataFile {
    private static final Logger logger = LoggerFactory.getLogger(DataFile.class);

    private final String filePath;
    private final String seriesName;
    private final long fileTimestamp;
    private RandomAccessFile file;
    private FileChannel channel;

    // 文件头结构（64字节）
    private static final int HEADER_SIZE = 64;
    private static final byte[] MAGIC_NUMBER = {0x54, 0x53, 0x44, 0x42}; // "TSDB"
    private static final int VERSION = 1;

    // 文件头字段偏移量
    private static final int OFFSET_MAGIC = 0;            // 4字节
    private static final int OFFSET_VERSION = 4;          // 4字节
    private static final int OFFSET_NAME_LEN = 8;         // 4字节
    private static final int OFFSET_NAME = 12;            // 变长
    private static final int OFFSET_MIN_TIME = 32;        // 8字节
    private static final int OFFSET_MAX_TIME = 40;        // 8字节
    private static final int OFFSET_POINT_COUNT = 48;     // 4字节
    private static final int OFFSET_BLOCK_COUNT = 52;     // 4字节
    private static final int OFFSET_COMPRESSION = 56;     // 1字节
    private static final int OFFSET_CHECKSUM = 57;        // 8字节（预留）

    // 数据记录格式
    private static final int RECORD_HEADER_SIZE = 20;     // 时间戳(8) + 位置(8) + 长度(4)

    // 缓存最近读取的索引，加速查询
    private final Map<Long, RecordInfo> recordCache = new HashMap<>();
    private volatile boolean cacheInitialized = false;

    public DataFile(String baseDir, String seriesName, long timestamp) throws IOException {
        this.seriesName = seriesName;
        this.fileTimestamp = timestamp;
        this.filePath = Paths.get(baseDir, "data",
                String.format("%s_%d.dat", seriesName, timestamp)).toString();
        initFile();
    }

    private void initFile() throws IOException {
        File dir = new File(filePath).getParentFile();
        if (!dir.exists()) {
            dir.mkdirs();
        }

        boolean fileExists = new File(filePath).exists();
        this.file = new RandomAccessFile(filePath, "rw");
        this.channel = file.getChannel();

        // 如果是新文件，写入头部
        if (!fileExists || file.length() == 0) {
            writeHeader();
            logger.debug("Created new data file: {}", filePath);
        } else {
            // 验证文件头
            if (!validateHeader()) {
                throw new IOException("Invalid data file format: " + filePath);
            }
            logger.debug("Opened existing data file: {}", filePath);
        }
    }

    private void writeHeader() throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(HEADER_SIZE);

        // 魔数
        buffer.put(MAGIC_NUMBER);

        // 版本
        buffer.putInt(VERSION);

        // 序列名称长度和内容
        byte[] nameBytes = seriesName.getBytes("UTF-8");
        buffer.putInt(nameBytes.length);
        buffer.put(nameBytes);

        // 时间范围（初始值）
        buffer.putLong(Long.MAX_VALUE); // 最小时间戳
        buffer.putLong(Long.MIN_VALUE); // 最大时间戳

        // 统计信息
        buffer.putInt(0);   // 数据点数量
        buffer.putInt(0);   // 数据块数量
        buffer.put((byte)0); // 压缩标志（0:未压缩，1:已压缩）

        // 校验和（预留）
        buffer.putLong(0L);

        // 填充剩余空间
        while (buffer.position() < HEADER_SIZE) {
            buffer.put((byte) 0);
        }

        buffer.flip();
        channel.write(buffer, 0);
        channel.force(true); // 强制刷写到磁盘
    }

    private boolean validateHeader() throws IOException {
        if (channel.size() < HEADER_SIZE) {
            return false;
        }

        ByteBuffer buffer = ByteBuffer.allocate(4);
        channel.read(buffer, OFFSET_MAGIC);
        buffer.flip();

        byte[] magic = new byte[4];
        buffer.get(magic);

        // 检查魔数
        return Arrays.equals(magic, MAGIC_NUMBER);
    }

    /**
     * 写入数据点
     */
    public void writeDataPoint(long timestamp, byte[] data) throws IOException {
        writeDataPoint(timestamp, data, false);
    }

    /**
     * 写入数据点（带压缩选项）
     */
    public void writeDataPoint(long timestamp, byte[] data, boolean compressed) throws IOException {
        long position = channel.size();

        // 准备数据缓冲区
        ByteBuffer buffer = ByteBuffer.allocate(RECORD_HEADER_SIZE + data.length);

        // 写入记录头
        buffer.putLong(timestamp);
        buffer.putLong(position);
        buffer.putInt(data.length);
        buffer.put((byte)(compressed ? 1 : 0)); // 压缩标志

        // 写入数据
        buffer.put(data);

        buffer.flip();

        // 写入文件
        channel.write(buffer);

        // 更新文件头统计信息
        updateHeaderStats(timestamp, data.length, compressed);

        // 添加到缓存
        recordCache.put(timestamp, new RecordInfo(position, data.length, compressed));

        logger.trace("Wrote data point at timestamp {} to position {}", timestamp, position);
    }

    /**
     * 批量写入数据点（性能优化）
     */
    public void writeDataPointsBatch(List<DataPointRecord> records) throws IOException {
        if (records == null || records.isEmpty()) {
            return;
        }

        // 按时间戳排序
        records.sort(Comparator.comparingLong(DataPointRecord::getTimestamp));

        long startPosition = channel.size();
        long minTime = Long.MAX_VALUE;
        long maxTime = Long.MIN_VALUE;
        int totalSize = 0;

        // 计算总大小并准备数据
        for (DataPointRecord record : records) {
            int recordSize = RECORD_HEADER_SIZE + record.getData().length;
            totalSize += recordSize;

            if (record.getTimestamp() < minTime) minTime = record.getTimestamp();
            if (record.getTimestamp() > maxTime) maxTime = record.getTimestamp();
        }

        // 分配缓冲区
        ByteBuffer buffer = ByteBuffer.allocate(totalSize);

        // 写入所有记录
        for (DataPointRecord record : records) {
            long position = startPosition + buffer.position();

            buffer.putLong(record.getTimestamp());
            buffer.putLong(position);
            buffer.putInt(record.getData().length);
            buffer.put((byte)(record.isCompressed() ? 1 : 0));
            buffer.put(record.getData());

            // 添加到缓存
            recordCache.put(record.getTimestamp(),
                    new RecordInfo(position, record.getData().length, record.isCompressed()));
        }

        buffer.flip();

        // 批量写入文件
        channel.write(buffer);

        // 更新统计信息
        updateHeaderStatsBatch(minTime, maxTime, records.size(), totalSize);

        logger.debug("Batch wrote {} data points to file {}", records.size(), filePath);
    }

    /**
     * 读取指定时间戳的数据
     */
    public byte[] readData(long timestamp) throws IOException {
        RecordInfo info = getRecordInfo(timestamp);
        if (info == null) {
            return null;
        }

        return readData(info.position, info.length, info.compressed);
    }

    /**
     * 读取指定位置的数据
     */
    public byte[] readData(long position, int length, boolean compressed) throws IOException {
        if (position < HEADER_SIZE || position + length > channel.size()) {
            throw new IOException("Invalid read position or length");
        }

        ByteBuffer buffer = ByteBuffer.allocate(length);
        channel.read(buffer, position);
        buffer.flip();

        byte[] data = new byte[length];
        buffer.get(data);

        // 如果需要解压
        if (compressed) {
            data = decompress(data);
        }

        return data;
    }

    /**
     * 读取指定时间范围内的所有数据
     */
    public List<byte[]> readDataPoints(long startTime, long endTime) throws IOException {
        List<byte[]> results = new ArrayList<>();

        // 初始化缓存（如果尚未初始化）
        if (!cacheInitialized) {
            loadRecordCache();
        }

        // 从缓存中查找在时间范围内的记录
        for (Map.Entry<Long, RecordInfo> entry : recordCache.entrySet()) {
            long timestamp = entry.getKey();
            if (timestamp >= startTime && timestamp <= endTime) {
                RecordInfo info = entry.getValue();
                try {
                    byte[] data = readData(info.position, info.length, info.compressed);
                    results.add(data);
                } catch (IOException e) {
                    logger.warn("Failed to read data for timestamp {}: {}", timestamp, e.getMessage());
                }
            }
        }

        return results;
    }

    /**
     * 批量读取数据点（性能优化）
     */
    public List<DataPointRecord> readDataPointsBatch(List<Long> timestamps) throws IOException {
        List<DataPointRecord> results = new ArrayList<>();

        if (timestamps == null || timestamps.isEmpty()) {
            return results;
        }

        // 按时间戳排序，提高读取效率
        List<Long> sortedTimestamps = new ArrayList<>(timestamps);
        Collections.sort(sortedTimestamps);

        // 初始化缓存（如果尚未初始化）
        if (!cacheInitialized) {
            loadRecordCache();
        }

        for (Long timestamp : sortedTimestamps) {
            RecordInfo info = recordCache.get(timestamp);
            if (info != null) {
                try {
                    byte[] data = readData(info.position, info.length, info.compressed);
                    results.add(new DataPointRecord(timestamp, data, info.compressed));
                } catch (IOException e) {
                    logger.warn("Failed to read data for timestamp {}: {}", timestamp, e.getMessage());
                }
            }
        }

        return results;
    }

    public List<byte[]> readDataPointsByIndex(List<IndexFile.IndexEntry> indexEntries) throws IOException {
        List<byte[]> results = new ArrayList<>();

        if (indexEntries == null || indexEntries.isEmpty()) {
            logger.warn("No index entries provided");
            return results;
        }

        logger.info("Reading {} data points by index", indexEntries.size());

        // 按文件偏移量排序，提高磁盘读取效率
        List<IndexFile.IndexEntry> sortedEntries = new ArrayList<>(indexEntries);
        sortedEntries.sort(Comparator.comparingLong(e -> e.fileOffset));

        for (int i = 0; i < sortedEntries.size(); i++) {
            IndexFile.IndexEntry entry = sortedEntries.get(i);
            try {
                logger.debug("Reading entry {}: offset={}, length={}",
                        i, entry.fileOffset, entry.dataLength);

                byte[] data = readData(entry.fileOffset, entry.dataLength, false);

                if (data != null) {
                    results.add(data);
                    logger.debug("Successfully read data for entry {}, size={}", i, data.length);
                } else {
                    logger.warn("Failed to read data for entry {}", i);
                }
            } catch (IOException e) {
                logger.error("Error reading data at offset {}: {}", entry.fileOffset, e.getMessage(), e);
            }
        }

        logger.info("Successfully read {} data points", results.size());
        return results;
    }

    /**
     * 获取记录信息（从缓存或文件）
     */
    private RecordInfo getRecordInfo(long timestamp) throws IOException {
        // 首先检查缓存
        RecordInfo cached = recordCache.get(timestamp);
        if (cached != null) {
            return cached;
        }

        // 缓存未命中，扫描文件（线性查找）
        long currentPos = HEADER_SIZE;
        ByteBuffer headerBuffer = ByteBuffer.allocate(RECORD_HEADER_SIZE);

        while (currentPos < channel.size()) {
            channel.read(headerBuffer, currentPos);
            headerBuffer.flip();

            long recordTimestamp = headerBuffer.getLong();
            long position = headerBuffer.getLong();
            int length = headerBuffer.getInt();
            boolean compressed = headerBuffer.get() == 1;

            headerBuffer.clear();

            // 添加到缓存
            recordCache.put(recordTimestamp, new RecordInfo(position, length, compressed));

            if (recordTimestamp == timestamp) {
                return new RecordInfo(position, length, compressed);
            }

            currentPos += RECORD_HEADER_SIZE + length;
        }

        return null; // 未找到
    }

    /**
     * 加载所有记录到缓存
     */
    private void loadRecordCache() throws IOException {
        recordCache.clear();

        long currentPos = HEADER_SIZE;
        ByteBuffer headerBuffer = ByteBuffer.allocate(RECORD_HEADER_SIZE);

        while (currentPos < channel.size()) {
            // 读取记录头
            channel.read(headerBuffer, currentPos);
            headerBuffer.flip();

            long timestamp = headerBuffer.getLong();
            long position = headerBuffer.getLong();
            int length = headerBuffer.getInt();
            boolean compressed = headerBuffer.get() == 1;

            headerBuffer.clear();

            // 添加到缓存
            recordCache.put(timestamp, new RecordInfo(position, length, compressed));

            // 跳过数据部分
            currentPos += RECORD_HEADER_SIZE + length;
        }

        cacheInitialized = true;
        logger.debug("Loaded {} records into cache for file {}", recordCache.size(), filePath);
    }

    private void updateHeaderStats(long timestamp, int dataSize, boolean compressed) throws IOException {
        // 读取当前统计信息
        ByteBuffer buffer = ByteBuffer.allocate(17); // 8+8+4+1
        channel.read(buffer, OFFSET_MIN_TIME);
        buffer.flip();

        long minTime = buffer.getLong();
        long maxTime = buffer.getLong();
        int pointCount = buffer.getInt();
        byte compressionFlag = buffer.get();

        // 更新
        if (timestamp < minTime) minTime = timestamp;
        if (timestamp > maxTime) maxTime = timestamp;
        pointCount++;

        // 更新压缩标志
        if (compressed) {
            compressionFlag = 1;
        }

        // 写回
        buffer.clear();
        buffer.putLong(minTime);
        buffer.putLong(maxTime);
        buffer.putInt(pointCount);
        buffer.put(compressionFlag);

        buffer.flip();
        channel.write(buffer, OFFSET_MIN_TIME);
    }

    private void updateHeaderStatsBatch(long minTime, long maxTime, int pointsAdded, int totalSize) throws IOException {
        // 读取当前统计信息
        ByteBuffer buffer = ByteBuffer.allocate(20); // 8+8+4
        channel.read(buffer, OFFSET_MIN_TIME);
        buffer.flip();

        long currentMin = buffer.getLong();
        long currentMax = buffer.getLong();
        int currentCount = buffer.getInt();

        // 更新
        if (minTime < currentMin) currentMin = minTime;
        if (maxTime > currentMax) currentMax = maxTime;
        currentCount += pointsAdded;

        // 更新块数（每4MB为一个块）
        int blockCount = (int)Math.ceil((double)channel.size() / (4 * 1024 * 1024));

        // 写回
        buffer.clear();
        buffer.putLong(currentMin);
        buffer.putLong(currentMax);
        buffer.putInt(currentCount);
        buffer.putInt(blockCount);

        buffer.flip();
        channel.write(buffer, OFFSET_MIN_TIME);
    }

    /**
     * 获取文件统计信息
     */
    public FileStats getFileStats() throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(20);
        channel.read(buffer, OFFSET_MIN_TIME);
        buffer.flip();

        long minTime = buffer.getLong();
        long maxTime = buffer.getLong();
        int pointCount = buffer.getInt();
        int blockCount = buffer.getInt();

        byte compressionFlag = 0;
        if (channel.size() >= OFFSET_COMPRESSION + 1) {
            buffer = ByteBuffer.allocate(1);
            channel.read(buffer, OFFSET_COMPRESSION);
            buffer.flip();
            compressionFlag = buffer.get();
        }

        return new FileStats(
                filePath,
                seriesName,
                fileTimestamp,
                minTime,
                maxTime,
                pointCount,
                blockCount,
                channel.size(),
                compressionFlag == 1
        );
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
            logger.warn("Compression failed, using original data", e);
            return data;
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
     * 关闭文件
     */
    public void close() throws IOException {
        try {
            if (channel != null && channel.isOpen()) {
                channel.force(true); // 强制刷写
                channel.close();
            }
        } finally {
            if (file != null) {
                file.close();
            }
            recordCache.clear();
            logger.debug("Closed data file: {}", filePath);
        }
    }

    /**
     * 强制刷写数据到磁盘
     */
    public void flush() throws IOException {
        if (channel != null && channel.isOpen()) {
            channel.force(true);
        }
    }

    /**
     * 获取文件路径
     */
    public String getFilePath() {
        return filePath;
    }

    /**
     * 获取文件大小
     */
    public long getFileSize() throws IOException {
        return channel != null ? channel.size() : 0;
    }

    /**
     * 检查文件是否有效
     */
    public boolean isValid() {
        try {
            return validateHeader();
        } catch (IOException e) {
            return false;
        }
    }

    // 内部类：记录信息
    private static class RecordInfo {
        final long position;
        final int length;
        final boolean compressed;

        RecordInfo(long position, int length, boolean compressed) {
            this.position = position;
            this.length = length;
            this.compressed = compressed;
        }
    }

    // 内部类：数据点记录
    public static class DataPointRecord {
        private final long timestamp;
        private final byte[] data;
        private final boolean compressed;

        public DataPointRecord(long timestamp, byte[] data, boolean compressed) {
            this.timestamp = timestamp;
            this.data = data;
            this.compressed = compressed;
        }

        public long getTimestamp() { return timestamp; }
        public byte[] getData() { return data; }
        public boolean isCompressed() { return compressed; }
    }

    // 内部类：文件统计信息
    public static class FileStats {
        private final String filePath;
        private final String seriesName;
        private final long fileTimestamp;
        private final long minTime;
        private final long maxTime;
        private final int pointCount;
        private final int blockCount;
        private final long fileSize;
        private final boolean compressed;

        public FileStats(String filePath, String seriesName, long fileTimestamp,
                         long minTime, long maxTime, int pointCount, int blockCount,
                         long fileSize, boolean compressed) {
            this.filePath = filePath;
            this.seriesName = seriesName;
            this.fileTimestamp = fileTimestamp;
            this.minTime = minTime;
            this.maxTime = maxTime;
            this.pointCount = pointCount;
            this.blockCount = blockCount;
            this.fileSize = fileSize;
            this.compressed = compressed;
        }

        // Getters
        public String getFilePath() { return filePath; }
        public String getSeriesName() { return seriesName; }
        public long getFileTimestamp() { return fileTimestamp; }
        public long getMinTime() { return minTime; }
        public long getMaxTime() { return maxTime; }
        public int getPointCount() { return pointCount; }
        public int getBlockCount() { return blockCount; }
        public long getFileSize() { return fileSize; }
        public boolean isCompressed() { return compressed; }

        public double getAveragePointSize() {
            return pointCount > 0 ? (double)(fileSize - HEADER_SIZE) / pointCount : 0;
        }

        public long getTimeRange() {
            return maxTime - minTime;
        }

        public double getPointsPerSecond() {
            long range = getTimeRange();
            return range > 0 ? (double)pointCount / (range / 1000.0) : 0;
        }

        @Override
        public String toString() {
            return String.format("FileStats{series=%s, points=%d, size=%.2fMB, range=%dms, compressed=%b}",
                    seriesName, pointCount, fileSize / (1024.0 * 1024.0), getTimeRange(), compressed);
        }
    }
}