package com.timeseries.db.test;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 时序库API高频并发测试用例
 * 覆盖：高并发写入、高并发查询、混合读写并发
 */
public class SimpleTimeSeriesApiConcurrentTest {
    // 全局配置（可根据测试需求调整）
    private static final String BASE_URL = "http://localhost:11002/api/timeseries";
    private static final int CONCURRENT_THREADS = 20; // 并发线程数
    private static final int REQUESTS_PER_THREAD = 100; // 每个线程执行的请求数
    private static final String TEST_MEASUREMENT = "concurrent_test_metric"; // 测试用指标名
    private static final long BASE_TIMESTAMP = System.currentTimeMillis(); // 基准时间戳

    // 统计指标
    private static final AtomicInteger successCount = new AtomicInteger(0); // 成功请求数
    private static final AtomicInteger failCount = new AtomicInteger(0); // 失败请求数
    private static final List<Long> responseTimes = new CopyOnWriteArrayList<>(); // 响应时间列表

    public static void main(String[] args) throws InterruptedException {
        System.out.println("===================== 时序库API高频并发测试 =====================");
        System.out.println("测试配置：");
        System.out.println("  - 并发线程数：" + CONCURRENT_THREADS);
        System.out.println("  - 每个线程请求数：" + REQUESTS_PER_THREAD);
        System.out.println("  - 总请求数：" + (CONCURRENT_THREADS * REQUESTS_PER_THREAD));
        System.out.println("  - 测试接口地址：" + BASE_URL);
        System.out.println("===============================================================\n");

        // 1. 高并发写入测试
        System.out.println("【测试1：高并发写入】");
        resetStatistics(); // 重置统计指标
        long writeStartTime = System.currentTimeMillis();
        executeConcurrentTask(new WriteTask());
        long writeEndTime = System.currentTimeMillis();
        printStatistics("高并发写入", writeStartTime, writeEndTime);

        // 等待1秒，确保数据落盘
        Thread.sleep(1000);

        // 2. 高并发查询测试
        System.out.println("\n【测试2：高并发查询】");
        resetStatistics();
        long queryStartTime = System.currentTimeMillis();
        executeConcurrentTask(new QueryTask());
        long queryEndTime = System.currentTimeMillis();
        printStatistics("高并发查询", queryStartTime, queryEndTime);

        // 3. 混合读写并发测试（50%写入 + 50%查询）
        System.out.println("\n【测试3：混合读写并发】");
        resetStatistics();
        long mixStartTime = System.currentTimeMillis();
        executeMixedTask();
        long mixEndTime = System.currentTimeMillis();
        printStatistics("混合读写并发", mixStartTime, mixEndTime);

        System.out.println("\n===================== 所有并发测试执行完成 =====================");
    }

    /**
     * 执行纯写入/纯查询的并发任务
     */
    private static void executeConcurrentTask(Runnable task) throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(CONCURRENT_THREADS);
        // 提交所有线程任务
        for (int i = 0; i < CONCURRENT_THREADS; i++) {
            executor.submit(task);
        }
        // 关闭线程池，等待所有任务完成
        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.MINUTES);
    }

    /**
     * 执行混合读写并发任务（50%写入 + 50%查询）
     */
    private static void executeMixedTask() throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(CONCURRENT_THREADS);
        // 提交混合任务：一半线程写入，一半线程查询
        int writeThreadCount = CONCURRENT_THREADS / 2;
        int queryThreadCount = CONCURRENT_THREADS - writeThreadCount;

        for (int i = 0; i < writeThreadCount; i++) {
            executor.submit(new WriteTask());
        }
        for (int i = 0; i < queryThreadCount; i++) {
            executor.submit(new QueryTask());
        }

        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.MINUTES);
    }

    /**
     * 重置统计指标
     */
    private static void resetStatistics() {
        successCount.set(0);
        failCount.set(0);
        responseTimes.clear();
    }

    /**
     * 打印测试统计结果
     */
    private static void printStatistics(String testType, long startTime, long endTime) {
        long totalTime = endTime - startTime;
        double qps = successCount.get() / (totalTime / 1000.0); // 每秒请求数
        long avgResponseTime = 0;
        long maxResponseTime = 0;
        long minResponseTime = Long.MAX_VALUE;

        if (!responseTimes.isEmpty()) {
            avgResponseTime = responseTimes.stream().mapToLong(Long::longValue).sum() / responseTimes.size();
            maxResponseTime = responseTimes.stream().mapToLong(Long::longValue).max().orElse(0);
            minResponseTime = responseTimes.stream().mapToLong(Long::longValue).min().orElse(0);
        }

        System.out.println("【" + testType + "统计结果】");
        System.out.println("  - 总耗时：" + totalTime + "ms");
        System.out.println("  - 成功请求数：" + successCount.get());
        System.out.println("  - 失败请求数：" + failCount.get());
        System.out.println("  - 成功率：" + String.format("%.2f%%", (successCount.get() * 100.0) / (successCount.get() + failCount.get())));
        System.out.println("  - QPS（每秒请求数）：" + String.format("%.2f", qps));
        System.out.println("  - 平均响应时间：" + avgResponseTime + "ms");
        System.out.println("  - 最大响应时间：" + maxResponseTime + "ms");
        System.out.println("  - 最小响应时间：" + minResponseTime + "ms");
    }

    // ===================== 并发任务定义 =====================

    /**
     * 写入任务：每个线程执行REQUESTS_PER_THREAD次写入请求
     */
    static class WriteTask implements Runnable {
        @Override
        public void run() {
            SimpleTimeSeriesApiClient client = new SimpleTimeSeriesApiClient(BASE_URL);
            Random random = new Random();

            for (int i = 0; i < REQUESTS_PER_THREAD; i++) {
                long startTime = System.currentTimeMillis();
                try {
                    // 构造唯一时间戳（线程ID + 循环索引 + 随机数，避免重复）
                    long timestamp = BASE_TIMESTAMP + (Thread.currentThread().getId() * 10000) + i + random.nextInt(1000);
                    // 随机生成指标值（0-100的浮点数）
                    double value = random.nextDouble() * 100;
                    // 执行写入
                    String result = client.write(TEST_MEASUREMENT, value, timestamp);
                    if ("success".equals(result)) {
                        successCount.incrementAndGet();
                    } else {
                        failCount.incrementAndGet();
                        System.err.println("线程[" + Thread.currentThread().getId() + "]写入失败，响应：" + result);
                    }
                } catch (Exception e) {
                    failCount.incrementAndGet();
                    System.err.println("线程[" + Thread.currentThread().getId() + "]写入异常：" + e.getMessage());
                } finally {
                    // 记录响应时间
                    long responseTime = System.currentTimeMillis() - startTime;
                    responseTimes.add(responseTime);
                }

                // 可选：添加微小延迟，模拟真实业务场景（避免瞬间压满服务器）
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    /**
     * 查询任务：每个线程执行REQUESTS_PER_THREAD次查询请求
     */
    static class QueryTask implements Runnable {
        @Override
        public void run() {
            SimpleTimeSeriesApiClient client = new SimpleTimeSeriesApiClient(BASE_URL);
            Random random = new Random();

            for (int i = 0; i < REQUESTS_PER_THREAD; i++) {
                long startTime = System.currentTimeMillis();
                try {
                    // 随机选择一个时间戳查询（覆盖写入的时间范围）
                    long targetTime = BASE_TIMESTAMP + random.nextInt((int) (CONCURRENT_THREADS * REQUESTS_PER_THREAD * 1000));
                    // 执行单指标实时值查询
                    SimpleTimeSeriesApiClient.SimpleDataPoint point = client.querySinglePoint(TEST_MEASUREMENT, targetTime);
                    // 只要不抛异常，就认为查询成功（无数据也是正常结果）
                    successCount.incrementAndGet();
                } catch (Exception e) {
                    failCount.incrementAndGet();
                    System.err.println("线程[" + Thread.currentThread().getId() + "]查询异常：" + e.getMessage());
                } finally {
                    long responseTime = System.currentTimeMillis() - startTime;
                    responseTimes.add(responseTime);
                }

                // 可选：添加微小延迟
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
}
