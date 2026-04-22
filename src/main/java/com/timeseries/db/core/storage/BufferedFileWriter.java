// ==================== 新增 BufferedFileWriter.java ====================
package com.timeseries.db.core.storage;

import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 文件写入缓冲池：减少文件打开/关闭次数
 */
@Slf4j
public class BufferedFileWriter implements AutoCloseable {

    private final String filePath;
    private final ReentrantLock lock = new ReentrantLock();
    private final BlockingQueue<String> writeQueue = new LinkedBlockingQueue<>(10000);
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private volatile boolean running = true;
    private BufferedWriter writer;
    private long lastWriteTime = System.currentTimeMillis();
    private int pendingCount = 0;

    private static final int BATCH_SIZE = 100;  // 批量写入阈值
    private static final long FLUSH_INTERVAL_MS = 100;  // 定时刷盘间隔

    public BufferedFileWriter(String filePath) throws IOException {
        this.filePath = filePath;
        ensureFileExists();
        openWriter();

        // 定时刷盘任务
        scheduler.scheduleAtFixedRate(this::scheduledFlush,
                FLUSH_INTERVAL_MS, FLUSH_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    private void ensureFileExists() throws IOException {
        File file = new File(filePath);
        File parent = file.getParentFile();
        if (parent != null && !parent.exists()) {
            parent.mkdirs();
        }
        if (!file.exists()) {
            file.createNewFile();
        }
    }

    private void openWriter() throws IOException {
        this.writer = new BufferedWriter(
                new OutputStreamWriter(
                        new FileOutputStream(filePath, true),
                        StandardCharsets.UTF_8),
                8192 * 4);  // 32KB 缓冲区
    }

    /**
     * 异步写入（立即返回）
     */
    public CompletableFuture<Void> writeAsync(String data) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        try {
            writeQueue.put(data);
            future.complete(null);
        } catch (InterruptedException e) {
            future.completeExceptionally(e);
            Thread.currentThread().interrupt();
        }
        return future;
    }

    /**
     * 批量异步写入
     */
    public CompletableFuture<Void> writeBatchAsync(List<String> dataList) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        try {
            for (String data : dataList) {
                writeQueue.put(data);
            }
            future.complete(null);
        } catch (InterruptedException e) {
            future.completeExceptionally(e);
            Thread.currentThread().interrupt();
        }
        return future;
    }

    /**
     * 同步写入（保证落盘）
     */
    public void writeSync(String data) {
        lock.lock();
        try {
            writer.write(data);
            writer.newLine();
            writer.flush();
        } catch (IOException e) {
            log.error("同步写入失败: {}", filePath, e);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 从队列消费并批量刷盘
     */
    private void scheduledFlush() {
        if (!running) return;

        List<String> batch = new ArrayList<>(BATCH_SIZE);
        writeQueue.drainTo(batch, BATCH_SIZE);

        if (batch.isEmpty() && pendingCount == 0) {
            return;
        }

        lock.lock();
        try {
            for (String data : batch) {
                writer.write(data);
                writer.newLine();
            }
            writer.flush();
            pendingCount = 0;
            lastWriteTime = System.currentTimeMillis();
        } catch (IOException e) {
            log.error("批量刷盘失败: {}", filePath, e);
            // 失败时重新放回队列
            for (String data : batch) {
                writeQueue.offer(data);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {
        running = false;
        scheduler.shutdown();
        lock.lock();
        try {
            scheduledFlush();  // 最后刷盘
            writer.close();
        } catch (IOException e) {
            log.error("关闭Writer失败", e);
        } finally {
            lock.unlock();
        }
    }
}
