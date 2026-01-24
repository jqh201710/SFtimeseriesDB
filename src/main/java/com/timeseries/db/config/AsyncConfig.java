// com.timeseries.db.config.AsyncConfig.java
package com.timeseries.db.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;

@Configuration
@EnableAsync
public class AsyncConfig {

    @Autowired
    private TimeSeriesConfig timeSeriesConfig;

    @Bean(name = "timeSeriesExecutor")
    public Executor timeSeriesExecutor() {
        TimeSeriesConfig.AsyncConfig asyncConfig = timeSeriesConfig.getAsync();
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        // 核心线程数
        executor.setCorePoolSize(asyncConfig.getCorePoolSize());
        // 最大线程数
        executor.setMaxPoolSize(asyncConfig.getMaxPoolSize());
        // 队列容量
        executor.setQueueCapacity(asyncConfig.getQueueCapacity());
        // 线程名前缀
        executor.setThreadNamePrefix("timeseries-async-");
        // 拒绝策略：调用者执行
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        // 初始化
        executor.initialize();
        return executor;
    }
}