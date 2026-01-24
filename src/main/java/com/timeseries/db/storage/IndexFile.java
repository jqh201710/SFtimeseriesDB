// storage/IndexFile.java - 索引文件
package com.timeseries.db.storage;

import com.timeseries.db.core.TimeSeriesDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.util.*;

public class IndexFile {
    private static final Logger logger = LoggerFactory.getLogger(IndexFile.class);

    private final String filePath;
    private RandomAccessFile file;
    private FileChannel channel;

    // 索引项结构：时间戳(8) + 文件偏移(8) + 数据长度(4) = 20字节
    private static final int INDEX_ENTRY_SIZE = 20;

    public IndexFile(String baseDir, String seriesName, long timestamp) throws IOException {
        this.filePath = Paths.get(baseDir, "index",
                String.format("%s_%d.idx", seriesName, timestamp)).toString();
        initFile();
    }

    private void initFile() throws IOException {
        File dir = new File(filePath).getParentFile();
        if (!dir.exists()) {
            dir.mkdirs();
        }

        this.file = new RandomAccessFile(filePath, "rw");
        this.channel = file.getChannel();
    }

    public void writeIndex(long timestamp, long fileOffset, int dataLength) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(INDEX_ENTRY_SIZE);
        buffer.putLong(timestamp);
        buffer.putLong(fileOffset);
        buffer.putInt(dataLength);

        buffer.flip();
        channel.write(buffer);
    }

    public List<IndexEntry> findInRange(long startTime, long endTime) throws IOException {
        List<IndexEntry> results = new ArrayList<>();

        long fileSize = channel.size();
        int entryCount = (int) (fileSize / INDEX_ENTRY_SIZE);

        logger.debug("Index file size: {}, entry count: {}", fileSize, entryCount);

        if (entryCount <= 0) {
            logger.warn("Index file has no entries: {}", filePath);
            return results;
        }

        // 二分查找开始位置
        int startIndex = binarySearch(startTime, 0, entryCount - 1);
        logger.debug("Binary search result: startIndex={}", startIndex);

        if (startIndex < 0 || startIndex >= entryCount) {
            return results;
        }

        // 线性扫描直到结束时间
        ByteBuffer buffer = ByteBuffer.allocate(INDEX_ENTRY_SIZE);
        for (int i = startIndex; i < entryCount; i++) {
            channel.read(buffer, i * INDEX_ENTRY_SIZE);
            buffer.flip();

            long timestamp = buffer.getLong();
            long offset = buffer.getLong();
            int length = buffer.getInt();

            logger.debug("Index entry {}: timestamp={}({}), offset={}, length={}",
                    i, timestamp, new Date(timestamp), offset, length);

            if (timestamp > endTime) {
                logger.debug("Timestamp {} > endTime {}, breaking loop", timestamp, endTime);
                break;
            }

            if (timestamp >= startTime) {
                results.add(new IndexEntry(timestamp, offset, length));
                logger.debug("Added index entry: timestamp={}", timestamp);
            }

            buffer.clear();
        }

        logger.info("Found {} index entries in range", results.size());
        return results;
    }

    private int binarySearch(long targetTime, int low, int high) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(8); // 只读取时间戳

        while (low <= high) {
            int mid = (low + high) >>> 1;

            channel.read(buffer, mid * INDEX_ENTRY_SIZE);
            buffer.flip();
            long midTime = buffer.getLong();
            buffer.clear();

            if (midTime < targetTime) {
                low = mid + 1;
            } else if (midTime > targetTime) {
                high = mid - 1;
            } else {
                return mid;
            }
        }

        return low; // 返回第一个大于等于目标时间的位置
    }

    public static class IndexEntry {
        public final long timestamp;
        public final long fileOffset;
        public final int dataLength;

        public IndexEntry(long timestamp, long fileOffset, int dataLength) {
            this.timestamp = timestamp;
            this.fileOffset = fileOffset;
            this.dataLength = dataLength;
        }
    }

    public void close() throws IOException {
        if (channel != null) channel.close();
        if (file != null) file.close();
    }
}