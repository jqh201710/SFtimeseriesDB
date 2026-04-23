
// ==================== 新增 BufferedFileWriterPool.java（带过期清理） ====================
package com.timeseries.db.core.storage;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalNotification;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * 文件写入器池：复用Writer实例，带自动过期清理，防止资源泄漏
 */
@Slf4j
@Component
public class BufferedFileWriterPool {

    private final Cache<String, BufferedFileWriter> writerPool;

    public BufferedFileWriterPool() {
        this.writerPool = CacheBuilder.newBuilder()
                .maximumSize(200)
                .expireAfterAccess(5, TimeUnit.MINUTES)
                .removalListener((RemovalNotification<String, BufferedFileWriter> notification) -> {
                    try {
                        if (notification.getValue() != null) {
                            notification.getValue().close();
                        }
                    } catch (Exception e) {
                        log.error("缓存淘汰时关闭Writer失败: {}", notification.getKey(), e);
                    }
                })
                .build();
    }

    /**
     * 获取或创建Writer
     */
    public BufferedFileWriter getWriter(String filePath) {
        try {
            return writerPool.get(filePath, () -> new BufferedFileWriter(filePath));
        } catch (ExecutionException e) {
            throw new RuntimeException("创建Writer失败: " + filePath, e);
        }
    }

    /**
     * 关闭所有Writer
     */
    @PreDestroy
    public void closeAll() {
        writerPool.invalidateAll();
        writerPool.cleanUp();
    }
}
