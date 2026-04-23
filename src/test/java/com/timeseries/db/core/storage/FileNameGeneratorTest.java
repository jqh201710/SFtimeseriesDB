package com.timeseries.db.core.storage;

import com.timeseries.db.config.TimeSeriesConfig;
import com.timeseries.db.core.model.Point;
import com.timeseries.db.core.model.TimeRange;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * FileNameGenerator 测试：验证 DateTimeFormatter 线程安全性
 */
public class FileNameGeneratorTest {

    private FileNameGenerator generator;

    @BeforeEach
    public void setUp() throws Exception {
        TimeSeriesConfig config = new TimeSeriesConfig();
        config.setBasePath("./test-data");
        config.setShardType("HOUR");
        generator = new FileNameGenerator();
        injectConfig(generator, config);
    }

    private void injectConfig(FileNameGenerator gen, TimeSeriesConfig config) throws Exception {
        Field field = FileNameGenerator.class.getDeclaredField("config");
        field.setAccessible(true);
        field.set(gen, config);
    }

    @Test
    public void testGenerateFilePath() {
        // 1706101200000 = 2024-01-24 21:00:00 CST
        Point point = new Point();
        point.setMeasurement("cpu");
        point.setTimestamp(1706101200000L);

        String path = generator.generateFilePath(point);
        assertEquals("./test-data/cpu/20240124/2024012421.json", path);
    }

    @Test
    public void testGenerateFilePathDayShard() throws Exception {
        TimeSeriesConfig config = new TimeSeriesConfig();
        config.setBasePath("./test-data");
        config.setShardType("DAY");
        FileNameGenerator dayGenerator = new FileNameGenerator();
        injectConfig(dayGenerator, config);

        Point point = new Point();
        point.setMeasurement("cpu");
        point.setTimestamp(1706101200000L); // 2024-01-24 21:00 CST

        String path = dayGenerator.generateFilePath(point);
        assertEquals("./test-data/cpu/20240124/20240124.json", path);
    }

    @Test
    public void testListFiles() {
        // 2024-01-24 21:00:00 -> 2024-01-24 23:00:00 (3 hours: 21, 22, 23)
        TimeRange range = new TimeRange(1706101200000L, 1706108400000L);
        List<String> files = generator.listFiles("cpu", range);

        assertEquals(3, files.size());
        assertTrue(files.get(0).contains("2024012421.json"));
        assertTrue(files.get(1).contains("2024012422.json"));
        assertTrue(files.get(2).contains("2024012423.json"));
    }

    @Test
    public void testConcurrentGenerateFilePath() throws InterruptedException {
        int threads = 20;
        int iterations = 1000;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(threads);
        AtomicInteger errorCount = new AtomicInteger(0);

        // 所有线程使用同一时间戳，测试并发安全性
        final long timestamp = 1706101200000L;
        for (int i = 0; i < threads; i++) {
            executor.submit(() -> {
                try {
                    Point point = new Point();
                    point.setMeasurement("cpu");
                    point.setTimestamp(timestamp);
                    for (int j = 0; j < iterations; j++) {
                        String path = generator.generateFilePath(point);
                        if (!path.equals("./test-data/cpu/20240124/2024012421.json")) {
                            errorCount.incrementAndGet();
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        boolean finished = latch.await(30, TimeUnit.SECONDS);
        executor.shutdown();

        assertTrue(finished, "并发测试应在30秒内完成");
        assertEquals(0, errorCount.get(), "DateTimeFormatter 线程安全，不应产生错误路径");
    }
}
