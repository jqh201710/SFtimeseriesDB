// com.timeseries.db.core.storage.FileNameGenerator.java
package com.timeseries.db.core.storage;

import com.timeseries.db.config.TimeSeriesConfig;
import com.timeseries.db.core.model.Point;
import com.timeseries.db.core.model.TimeRange;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

@Component
public class FileNameGenerator {

    @Autowired
    private TimeSeriesConfig config;

    private static final String HOUR_FORMAT = "yyyyMMddHH";
    private static final String DAY_FORMAT = "yyyyMMdd";
    private static final SimpleDateFormat HOUR_SDF = new SimpleDateFormat(HOUR_FORMAT);
    private static final SimpleDateFormat DAY_SDF = new SimpleDateFormat(DAY_FORMAT);

    /**
     * 生成单个Point对应的文件路径
     */
    public String generateFilePath(Point point) {
        String timeFormat = "HOUR".equals(config.getShardType()) ? HOUR_FORMAT : DAY_FORMAT;
        SimpleDateFormat sdf = "HOUR".equals(config.getShardType()) ? HOUR_SDF : DAY_SDF;
        String timeStr = sdf.format(new Date(point.getTimestamp()));
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
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date(timeRange.getStart()));

        Calendar endCal = Calendar.getInstance();
        endCal.setTime(new Date(timeRange.getEnd()));

        // 按分片类型遍历时间范围，生成所有可能的文件路径
        while (cal.before(endCal) || cal.equals(endCal)) {
            String timeFormat = "HOUR".equals(config.getShardType()) ? HOUR_FORMAT : DAY_FORMAT;
            SimpleDateFormat sdf = "HOUR".equals(config.getShardType()) ? HOUR_SDF : DAY_SDF;
            String timeStr = sdf.format(cal.getTime());
            String filePath = String.format("%s/%s/%s/%s.json",
                    config.getBasePath(),
                    measurement,
                    timeStr.substring(0, 8),
                    timeStr);
            filePaths.add(filePath);

            // 按小时/天递增
            if ("HOUR".equals(config.getShardType())) {
                cal.add(Calendar.HOUR, 1);
            } else {
                cal.add(Calendar.DAY_OF_MONTH, 1);
            }
        }
        return filePaths;
    }
}