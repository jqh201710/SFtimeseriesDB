// storage/MetaFile.java - JDK 1.8兼容的元数据管理
package com.timeseries.db.storage;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONReader;
import com.alibaba.fastjson2.JSONWriter;
import com.alibaba.fastjson2.TypeReference;
import com.alibaba.fastjson2.filter.PropertyFilter;

import java.io.*;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class MetaFile {
    private final String metaDir;
    private final Map<String, SeriesMeta> seriesCache;

    // FastJSON2配置（移除WriteDateUseDateFormat，使用JSONWriter.Feature.WriteDateAsTimestamp代替）
    private static final JSONWriter.Feature[] WRITE_FEATURES = {
            JSONWriter.Feature.PrettyFormat,
            JSONWriter.Feature.WriteEnumsUsingName,
            JSONWriter.Feature.WriteMapNullValue
//            JSONWriter.Feature.WriteDateAsTimestamp  // 使用时间戳格式替代WriteDateUseDateFormat
    };

    private static final JSONReader.Feature[] READ_FEATURES = {
            JSONReader.Feature.SupportSmartMatch,
            JSONReader.Feature.AllowUnQuotedFieldNames,
            JSONReader.Feature.SupportArrayToBean,
            JSONReader.Feature.UseNativeObject  // 添加此特性以提高性能
    };

    // 类型引用，用于反序列化泛型
    private static final Type SERIES_META_TYPE = new TypeReference<SeriesMeta>(){}.getType();
    private static final Type DATABASE_CONFIG_TYPE = new TypeReference<DatabaseConfig>(){}.getType();
    private static final Type MAP_STRING_SERIES_INDEX_TYPE = new TypeReference<Map<String, SeriesIndex>>(){}.getType();

    public MetaFile(String baseDir) {
        this.metaDir = Paths.get(baseDir, "meta").toString();
        this.seriesCache = new ConcurrentHashMap<>();

        // 确保目录存在
        ensureDirectoryExists();
        loadAllSeries();
    }

    /**
     * 确保目录存在（JDK 1.8兼容写法）
     */
    private void ensureDirectoryExists() {
        File metaDirFile = new File(metaDir);
        if (!metaDirFile.exists()) {
            metaDirFile.mkdirs();
        }

        File seriesDir = new File(metaDir, "series");
        if (!seriesDir.exists()) {
            seriesDir.mkdirs();
        }
    }

    /**
     * 保存序列元数据
     */
    public void saveSeriesMeta(String seriesName, SeriesMeta meta) throws IOException {
        seriesCache.put(seriesName, meta);

        String filePath = Paths.get(metaDir, "series", seriesName + ".json").toString();
        File file = new File(filePath);

        // 确保父目录存在
        File parent = file.getParentFile();
        if (parent != null && !parent.exists()) {
            parent.mkdirs();
        }

        // 使用FastJSON序列化
        String json = JSON.toJSONString(meta, WRITE_FEATURES);

        // JDK 1.8兼容的文件写入（替代Files.writeString）
        writeStringToFile(file, json);

        // 同时更新索引文件
        updateSeriesIndex(seriesName, meta);
    }

    /**
     * 获取序列元数据
     */
    public SeriesMeta getSeriesMeta(String seriesName) throws IOException {
        SeriesMeta meta = seriesCache.get(seriesName);
        if (meta != null) {
            return meta;
        }

        String filePath = Paths.get(metaDir, "series", seriesName + ".json").toString();
        File file = new File(filePath);
        if (file.exists()) {
            try {
                // JDK 1.8兼容的文件读取（替代Files.readString）
                String json = readStringFromFile(file);

                // 使用FastJSON反序列化
                meta = JSON.parseObject(json, SERIES_META_TYPE, READ_FEATURES);
                if (meta != null) {
                    meta.setName(seriesName); // 确保名称正确
                    seriesCache.put(seriesName, meta);
                }
                return meta;
            } catch (Exception e) {
                throw new IOException("Failed to parse series meta for " + seriesName, e);
            }
        }

        return null;
    }

    /**
     * 批量保存序列元数据（性能优化）
     */
    public void saveSeriesMetaBatch(Map<String, SeriesMeta> metas) throws IOException {
        for (Map.Entry<String, SeriesMeta> entry : metas.entrySet()) {
            seriesCache.put(entry.getKey(), entry.getValue());
        }

        // 批量写入文件
        for (Map.Entry<String, SeriesMeta> entry : metas.entrySet()) {
            String filePath = Paths.get(metaDir, "series", entry.getKey() + ".json").toString();
            File file = new File(filePath);

            File parent = file.getParentFile();
            if (parent != null && !parent.exists()) {
                parent.mkdirs();
            }

            String json = JSON.toJSONString(entry.getValue(), WRITE_FEATURES);
            writeStringToFile(file, json);
        }

        // 批量更新索引
        updateSeriesIndexBatch(metas);
    }

    /**
     * 获取所有序列名称
     */
    public List<String> getAllSeries() {
        return new ArrayList<>(seriesCache.keySet());
    }

    /**
     * 获取所有序列元数据（带过滤）
     */
    public List<SeriesMeta> getAllSeriesMeta(Map<String, String> tagFilters) {
        List<SeriesMeta> result = new ArrayList<>();

        for (SeriesMeta meta : seriesCache.values()) {
            if (matchTags(meta.getTags(), tagFilters)) {
                result.add(meta);
            }
        }

        // 按最后更新时间排序（JDK 1.8兼容写法）
        Collections.sort(result, new Comparator<SeriesMeta>() {
            @Override
            public int compare(SeriesMeta a, SeriesMeta b) {
                return Long.compare(b.getLastUpdated(), a.getLastUpdated());
            }
        });

        return result;
    }

    /**
     * 根据标签过滤序列
     */
    public List<String> getSeriesByTags(Map<String, String> tags) {
        List<String> result = new ArrayList<>();

        for (Map.Entry<String, SeriesMeta> entry : seriesCache.entrySet()) {
            if (matchTags(entry.getValue().getTags(), tags)) {
                result.add(entry.getKey());
            }
        }

        return result;
    }

    /**
     * 删除序列元数据
     */
    public void deleteSeriesMeta(String seriesName) throws IOException {
        seriesCache.remove(seriesName);

        String filePath = Paths.get(metaDir, "series", seriesName + ".json").toString();
        File file = new File(filePath);
        if (file.exists()) {
            if (!file.delete()) {
                throw new IOException("Failed to delete file: " + filePath);
            }
        }

        // 从索引中删除
        removeFromSeriesIndex(seriesName);
    }

    /**
     * 加载所有序列元数据（JDK 1.8兼容写法）
     */
    private void loadAllSeries() {
        String seriesDirPath = Paths.get(metaDir, "series").toString();
        File seriesDir = new File(seriesDirPath);

        if (!seriesDir.exists() || !seriesDir.isDirectory()) {
            return;
        }

        File[] files = seriesDir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(".json");
            }
        });

        if (files != null) {
            for (File file : files) {
                try {
                    // JDK 1.8兼容的文件读取
                    String json = readStringFromFile(file);

                    SeriesMeta meta = JSON.parseObject(json, SERIES_META_TYPE, READ_FEATURES);
                    if (meta != null) {
                        String seriesName = file.getName().replace(".json", "");
                        meta.setName(seriesName);
                        seriesCache.put(seriesName, meta);
                    }
                } catch (Exception e) {
                    System.err.println("Failed to load meta for " + file.getName() + ": " + e.getMessage());
                }
            }
        }

        // 加载索引文件以验证一致性
        loadSeriesIndex();
    }

    /**
     * 保存数据库配置
     */
    public void saveDatabaseConfig(DatabaseConfig config) throws IOException {
        String filePath = Paths.get(metaDir, "config.json").toString();
        File file = new File(filePath);

        // 添加额外的安全特性
        JSONWriter.Feature[] secureFeatures = Arrays.copyOf(WRITE_FEATURES, WRITE_FEATURES.length + 1);
        secureFeatures[secureFeatures.length - 1] = JSONWriter.Feature.BrowserSecure;

        String json = JSON.toJSONString(config, secureFeatures);
        writeStringToFile(file, json);
    }

    /**
     * 加载数据库配置
     */
    public DatabaseConfig loadDatabaseConfig() throws IOException {
        String filePath = Paths.get(metaDir, "config.json").toString();
        File file = new File(filePath);

        if (file.exists()) {
            try {
                String json = readStringFromFile(file);
                return JSON.parseObject(json, DATABASE_CONFIG_TYPE, READ_FEATURES);
            } catch (Exception e) {
                throw new IOException("Failed to parse database config", e);
            }
        }

        // 返回默认配置并保存
        DatabaseConfig defaultConfig = new DatabaseConfig();
        saveDatabaseConfig(defaultConfig);
        return defaultConfig;
    }

    /**
     * 保存统计信息
     */
    public void saveStats(DatabaseStats stats) throws IOException {
        String filePath = Paths.get(metaDir, "stats.json").toString();
        File file = new File(filePath);

        // 使用自定义过滤器
        PropertyFilter filter = new PropertyFilter() {
            @Override
            public boolean apply(Object object, String name, Object value) {
                // 过滤掉null值
                return value != null;
            }
        };

        String json = JSON.toJSONString(stats, filter, WRITE_FEATURES);
        writeStringToFile(file, json);
    }

    /**
     * 加载统计信息
     */
    public DatabaseStats loadStats() throws IOException {
        String filePath = Paths.get(metaDir, "stats.json").toString();
        File file = new File(filePath);

        if (file.exists()) {
            try {
                String json = readStringFromFile(file);
                return JSON.parseObject(json, DatabaseStats.class, READ_FEATURES);
            } catch (Exception e) {
                throw new IOException("Failed to parse stats", e);
            }
        }

        return new DatabaseStats();
    }

    /**
     * 更新序列索引
     */
    private void updateSeriesIndex(String seriesName, SeriesMeta meta) throws IOException {
        String indexPath = Paths.get(metaDir, "series_index.json").toString();
        File indexFile = new File(indexPath);

        Map<String, SeriesIndex> index = loadSeriesIndex();

        SeriesIndex seriesIndex = new SeriesIndex();
        seriesIndex.setName(seriesName);
        seriesIndex.setCreatedTime(meta.getCreatedTime());
        seriesIndex.setLastUpdated(meta.getLastUpdated());
        seriesIndex.setTotalPoints(meta.getTotalPoints());
        seriesIndex.setTags(meta.getTags());

        index.put(seriesName, seriesIndex);

        // 保存索引（使用非美化格式以节省空间）
        String json = JSON.toJSONString(index);
        writeStringToFile(indexFile, json);
    }

    /**
     * 批量更新序列索引
     */
    private void updateSeriesIndexBatch(Map<String, SeriesMeta> metas) throws IOException {
        String indexPath = Paths.get(metaDir, "series_index.json").toString();
        File indexFile = new File(indexPath);

        Map<String, SeriesIndex> index = loadSeriesIndex();

        for (Map.Entry<String, SeriesMeta> entry : metas.entrySet()) {
            SeriesMeta meta = entry.getValue();
            SeriesIndex seriesIndex = new SeriesIndex();
            seriesIndex.setName(entry.getKey());
            seriesIndex.setCreatedTime(meta.getCreatedTime());
            seriesIndex.setLastUpdated(meta.getLastUpdated());
            seriesIndex.setTotalPoints(meta.getTotalPoints());
            seriesIndex.setTags(meta.getTags());

            index.put(entry.getKey(), seriesIndex);
        }

        String json = JSON.toJSONString(index);
        writeStringToFile(indexFile, json);
    }

    /**
     * 从索引中移除序列
     */
    private void removeFromSeriesIndex(String seriesName) throws IOException {
        String indexPath = Paths.get(metaDir, "series_index.json").toString();
        File indexFile = new File(indexPath);

        if (!indexFile.exists()) {
            return;
        }

        Map<String, SeriesIndex> index = loadSeriesIndex();
        index.remove(seriesName);

        String json = JSON.toJSONString(index);
        writeStringToFile(indexFile, json);
    }

    /**
     * 加载序列索引
     */
    private Map<String, SeriesIndex> loadSeriesIndex() {
        String indexPath = Paths.get(metaDir, "series_index.json").toString();
        File indexFile = new File(indexPath);

        if (!indexFile.exists()) {
            return new ConcurrentHashMap<>();
        }

        try {
            String json = readStringFromFile(indexFile);
            return JSON.parseObject(json, MAP_STRING_SERIES_INDEX_TYPE, READ_FEATURES);
        } catch (Exception e) {
            System.err.println("Failed to load series index: " + e.getMessage());
            return new ConcurrentHashMap<>();
        }
    }

    /**
     * JDK 1.8兼容的文件写入方法（替代Files.writeString）
     */
    private void writeStringToFile(File file, String content) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(file), StandardCharsets.UTF_8))) {
            writer.write(content);
        }
    }

    /**
     * JDK 1.8兼容的文件读取方法（替代Files.readString）
     */
    private String readStringFromFile(File file) throws IOException {
        StringBuilder content = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                content.append(line).append(System.lineSeparator());
            }
        }
        return content.toString();
    }

    /**
     * 标签匹配
     */
    private boolean matchTags(Map<String, String> pointTags, Map<String, String> filterTags) {
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
     * 清理旧元数据
     */
    public void cleanupOldMeta(long retentionDays) throws IOException {
        long cutoffTime = System.currentTimeMillis() - (retentionDays * 24 * 60 * 60 * 1000L);

        List<String> toRemove = new ArrayList<>();
        for (Map.Entry<String, SeriesMeta> entry : seriesCache.entrySet()) {
            if (entry.getValue().getLastUpdated() < cutoffTime) {
                toRemove.add(entry.getKey());
            }
        }

        for (String seriesName : toRemove) {
            deleteSeriesMeta(seriesName);
        }
    }

    // ========== 内部类定义 ==========

    public static class SeriesMeta {
        private String name;
        private long createdTime;
        private long lastUpdated;
        private long totalPoints;
        private Map<String, String> tags;
        private List<DataFileInfo> dataFiles;

        // FastJSON兼容的构造函数
        public SeriesMeta() {
            this.tags = new HashMap<>();
            this.dataFiles = new ArrayList<>();
        }

        public SeriesMeta(String name) {
            this();
            this.name = name;
            this.createdTime = System.currentTimeMillis();
            this.lastUpdated = this.createdTime;
        }

        // Getters and Setters
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }

        public long getCreatedTime() { return createdTime; }
        public void setCreatedTime(long createdTime) { this.createdTime = createdTime; }

        public long getLastUpdated() { return lastUpdated; }
        public void setLastUpdated(long lastUpdated) {
            this.lastUpdated = lastUpdated;
        }

        public long getTotalPoints() { return totalPoints; }
        public void setTotalPoints(long totalPoints) { this.totalPoints = totalPoints; }

        public Map<String, String> getTags() { return tags; }
        public void setTags(Map<String, String> tags) { this.tags = tags; }

        public List<DataFileInfo> getDataFiles() { return dataFiles; }
        public void setDataFiles(List<DataFileInfo> dataFiles) { this.dataFiles = dataFiles; }

        /**
         * 添加标签
         */
        public void addTag(String key, String value) {
            if (this.tags == null) {
                this.tags = new HashMap<>();
            }
            this.tags.put(key, value);
            this.lastUpdated = System.currentTimeMillis();
        }

        /**
         * 添加数据文件信息
         */
        public void addDataFile(DataFileInfo fileInfo) {
            if (this.dataFiles == null) {
                this.dataFiles = new ArrayList<>();
            }
            this.dataFiles.add(fileInfo);
            this.lastUpdated = System.currentTimeMillis();
        }

        /**
         * 更新统计信息
         */
        public void updateStats(int pointsAdded) {
            this.totalPoints += pointsAdded;
            this.lastUpdated = System.currentTimeMillis();
        }
    }

    public static class DataFileInfo {
        private String fileName;
        private long startTime;
        private long endTime;
        private long fileSize;
        private int pointCount;
        private String compression; // 压缩算法
        private String checksum;    // 文件校验和

        // Getters and Setters
        public String getFileName() { return fileName; }
        public void setFileName(String fileName) { this.fileName = fileName; }

        public long getStartTime() { return startTime; }
        public void setStartTime(long startTime) { this.startTime = startTime; }

        public long getEndTime() { return endTime; }
        public void setEndTime(long endTime) { this.endTime = endTime; }

        public long getFileSize() { return fileSize; }
        public void setFileSize(long fileSize) { this.fileSize = fileSize; }

        public int getPointCount() { return pointCount; }
        public void setPointCount(int pointCount) { this.pointCount = pointCount; }

        public String getCompression() { return compression; }
        public void setCompression(String compression) { this.compression = compression; }

        public String getChecksum() { return checksum; }
        public void setChecksum(String checksum) { this.checksum = checksum; }
    }

    public static class DatabaseConfig {
        private int maxMemoryPoints = 10000;
        private boolean compressionEnabled = true;
        private int flushIntervalSeconds = 30;
        private int compactionIntervalHours = 1;
        private int retentionDays = 30;
        private int maxSeriesCount = 10000;
        private boolean enableWAL = true;
        private String compressionAlgorithm = "gzip";
        private int blockSizeKB = 4096;

        // Getters and Setters
        public int getMaxMemoryPoints() { return maxMemoryPoints; }
        public void setMaxMemoryPoints(int maxMemoryPoints) { this.maxMemoryPoints = maxMemoryPoints; }

        public boolean isCompressionEnabled() { return compressionEnabled; }
        public void setCompressionEnabled(boolean compressionEnabled) { this.compressionEnabled = compressionEnabled; }

        public int getFlushIntervalSeconds() { return flushIntervalSeconds; }
        public void setFlushIntervalSeconds(int flushIntervalSeconds) { this.flushIntervalSeconds = flushIntervalSeconds; }

        public int getCompactionIntervalHours() { return compactionIntervalHours; }
        public void setCompactionIntervalHours(int compactionIntervalHours) { this.compactionIntervalHours = compactionIntervalHours; }

        public int getRetentionDays() { return retentionDays; }
        public void setRetentionDays(int retentionDays) { this.retentionDays = retentionDays; }

        public int getMaxSeriesCount() { return maxSeriesCount; }
        public void setMaxSeriesCount(int maxSeriesCount) { this.maxSeriesCount = maxSeriesCount; }

        public boolean isEnableWAL() { return enableWAL; }
        public void setEnableWAL(boolean enableWAL) { this.enableWAL = enableWAL; }

        public String getCompressionAlgorithm() { return compressionAlgorithm; }
        public void setCompressionAlgorithm(String compressionAlgorithm) { this.compressionAlgorithm = compressionAlgorithm; }

        public int getBlockSizeKB() { return blockSizeKB; }
        public void setBlockSizeKB(int blockSizeKB) { this.blockSizeKB = blockSizeKB; }
    }

    public static class DatabaseStats {
        private long totalSeries;
        private long totalPoints;
        private long totalDataSize;
        private long totalIndexSize;
        private long startupTime;
        private long lastCompactTime;
        private Map<String, Long> tagStatistics;
        private List<SeriesStats> topSeries;

        public DatabaseStats() {
            this.tagStatistics = new HashMap<>();
            this.topSeries = new ArrayList<>();
            this.startupTime = System.currentTimeMillis();
        }

        // Getters and Setters
        public long getTotalSeries() { return totalSeries; }
        public void setTotalSeries(long totalSeries) { this.totalSeries = totalSeries; }

        public long getTotalPoints() { return totalPoints; }
        public void setTotalPoints(long totalPoints) { this.totalPoints = totalPoints; }

        public long getTotalDataSize() { return totalDataSize; }
        public void setTotalDataSize(long totalDataSize) { this.totalDataSize = totalDataSize; }

        public long getTotalIndexSize() { return totalIndexSize; }
        public void setTotalIndexSize(long totalIndexSize) { this.totalIndexSize = totalIndexSize; }

        public long getStartupTime() { return startupTime; }
        public void setStartupTime(long startupTime) { this.startupTime = startupTime; }

        public long getLastCompactTime() { return lastCompactTime; }
        public void setLastCompactTime(long lastCompactTime) { this.lastCompactTime = lastCompactTime; }

        public Map<String, Long> getTagStatistics() { return tagStatistics; }
        public void setTagStatistics(Map<String, Long> tagStatistics) { this.tagStatistics = tagStatistics; }

        public List<SeriesStats> getTopSeries() { return topSeries; }
        public void setTopSeries(List<SeriesStats> topSeries) { this.topSeries = topSeries; }

        /**
         * 更新统计信息
         */
        public void update(String seriesName, int pointsAdded, long dataSize) {
            this.totalPoints += pointsAdded;
            this.totalDataSize += dataSize;
        }
    }

    public static class SeriesStats {
        private String name;
        private long pointCount;
        private long dataSize;
        private long lastUpdate;

        // Getters and Setters
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }

        public long getPointCount() { return pointCount; }
        public void setPointCount(long pointCount) { this.pointCount = pointCount; }

        public long getDataSize() { return dataSize; }
        public void setDataSize(long dataSize) { this.dataSize = dataSize; }

        public long getLastUpdate() { return lastUpdate; }
        public void setLastUpdate(long lastUpdate) { this.lastUpdate = lastUpdate; }
    }

    public static class SeriesIndex {
        private String name;
        private long createdTime;
        private long lastUpdated;
        private long totalPoints;
        private Map<String, String> tags;

        // Getters and Setters
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }

        public long getCreatedTime() { return createdTime; }
        public void setCreatedTime(long createdTime) { this.createdTime = createdTime; }

        public long getLastUpdated() { return lastUpdated; }
        public void setLastUpdated(long lastUpdated) { this.lastUpdated = lastUpdated; }

        public long getTotalPoints() { return totalPoints; }
        public void setTotalPoints(long totalPoints) { this.totalPoints = totalPoints; }

        public Map<String, String> getTags() { return tags; }
        public void setTags(Map<String, String> tags) { this.tags = tags; }
    }
}