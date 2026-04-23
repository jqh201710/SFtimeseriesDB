// com.timeseries.db.core.storage.FileNameGenerator.java
package com.timeseries.db.core.storage;

import com.timeseries.db.config.TimeSeriesConfig;
import com.timeseries.db.core.model.Point;
import com.timeseries.db.core.model.TimeRange;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

@Component
public class FileNameGenerator {

    @Autowired
    private TimeSeriesConfig config;

    private static final String HOUR_FORMAT = "yyyyMMddHH";
    private static final String DAY_FORMAT = "yyyyMMdd";
    private static final DateTimeFormatter HOUR_DTF = DateTimeFormatter.ofPattern(HOUR_FORMAT).withZone(ZoneId.systemDefault());
    private static final DateTimeFormatter DAY_DTF = DateTimeFormatter.ofPattern(DAY_FORMAT).withZone(ZoneId.systemDefault());

    /**
     * 生成单个Point对应的文件路径
     */
    public String generateFilePath(Point point) {
        String timeStr = "HOUR".equals(config.getShardType())
                ? HOUR_DTF.format(Instant.ofEpochMilli(point.getTimestamp()))
                : DAY_DTF.format(Instant.ofEpochMilli(point.getTimestamp()));
        // 路径格式：basePath/measurement/20240124/2024012401.json
        return String.format("%s/%s/%s/%s.json",
                config.getBasePath(),
                point.getMeasurement(),
                timeStr.substring(0, 8), // 年月日
                timeStr);
    }

    /**
     * 列出时间范围内的所有文件路径
     */
    public List<String> listFiles(String measurement, TimeRange timeRange) {
        List<String> filePaths = new ArrayList<>();
        long current = truncateToShardBoundary(timeRange.getStart());
        long end = truncateToShardBoundary(timeRange.getEnd());

        // 按分片类型遍历时间范围，生成所有可能的文件路径
        while (current <= end) {
            String timeStr = "HOUR".equals(config.getShardType())
                    ? HOUR_DTF.format(Instant.ofEpochMilli(current))
                    : DAY_DTF.format(Instant.ofEpochMilli(current));
            String filePath = String.format("%s/%s/%s/%s.json",
                    config.getBasePath(),
                    measurement,
                    timeStr.substring(0, 8),
                    timeStr);
            filePaths.add(filePath);

            // 按小时/天递增
            if ("HOUR".equals(config.getShardType())) {
                current += 3600_000L; // +1 hour in millis
            } else {
                current += 86_400_000L; // +1 day in millis
            }
        }
        return filePaths;
    }

    /**
     * 将时间戳截断到当前分片类型的起始边界（整小时或整天）
     */
    private long truncateToShardBoundary(long timestamp) {
        if ("HOUR".equals(config.getShardType())) {
            return (timestamp / 3600_000L) * 3600_000L;
        } else {
            // 获取当前时区当天的开始时间戳
            java.time.LocalDate date = Instant.ofEpochMilli(timestamp).atZone(ZoneId.systemDefault()).toLocalDate();
            return date.atStartOfDay(ZoneId.systemDefault()).toInstant().toEpochMilli();
        }
    }
}
