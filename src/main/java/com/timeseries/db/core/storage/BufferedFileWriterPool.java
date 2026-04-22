
// ==================== 新增 BufferedFileWriterPool.java ====================
package com.timeseries.db.core.storage;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 文件写入器池：复用Writer实例
 */
@Slf4j
@Component
public class BufferedFileWriterPool {

    private final ConcurrentHashMap<String, BufferedFileWriter> writerPool = new ConcurrentHashMap<>();

    /**
     * 获取或创建Writer
     */
    public BufferedFileWriter getWriter(String filePath) {
        return writerPool.computeIfAbsent(filePath, path -> {
            try {
                return new BufferedFileWriter(path);
            } catch (IOException e) {
                log.error("创建Writer失败: {}", path, e);
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * 关闭所有Writer
     */
    @PreDestroy
    public void closeAll() {
        writerPool.values().forEach(writer -> {
            try {
                writer.close();
            } catch (Exception e) {
                log.error("关闭Writer失败", e);
            }
        });
        writerPool.clear();
    }
}