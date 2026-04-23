// ==================== 新增 BufferedFileWriter.java（简化版） ====================
package com.timeseries.db.core.storage;

import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 文件写入缓冲器：复用 BufferedWriter，减少文件打开/关闭次数
 */
@Slf4j
public class BufferedFileWriter implements AutoCloseable {

    private final String filePath;
    private final ReentrantLock lock = new ReentrantLock();
    private BufferedWriter writer;

    public BufferedFileWriter(String filePath) throws IOException {
        this.filePath = filePath;
        ensureFileExists();
        openWriter();
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
            throw new RuntimeException("同步写入失败: " + filePath, e);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 批量同步写入（保证落盘）
     */
    public void writeBatchSync(List<String> dataList) {
        if (dataList == null || dataList.isEmpty()) {
            return;
        }
        lock.lock();
        try {
            for (String data : dataList) {
                writer.write(data);
                writer.newLine();
            }
            writer.flush();
        } catch (IOException e) {
            log.error("批量同步写入失败: {}", filePath, e);
            throw new RuntimeException("批量同步写入失败: " + filePath, e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {
        lock.lock();
        try {
            if (writer != null) {
                writer.flush();
                writer.close();
            }
        } catch (IOException e) {
            log.error("关闭Writer失败: {}", filePath, e);
        } finally {
            lock.unlock();
        }
    }
}
