/*
 * Copyright (c) 2025-2025 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *           http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.full.migration.strategy;

import org.full.migration.coordinator.ProgressTracker;
import org.full.migration.coordinator.QueueManager;
import org.full.migration.source.SourceDatabase;
import org.full.migration.target.TargetDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * MigrationStrategy
 *
 * @since 2025-04-18
 */
public abstract class MigrationStrategy {
    private static final Logger LOGGER = LoggerFactory.getLogger(TableMigration.class);

    /**
     * source
     */
    protected final SourceDatabase source;

    /**
     * target
     */
    protected final TargetDatabase target;

    /**
     * MigrationStrategy
     *
     * @param source source
     * @param target target
     */
    public MigrationStrategy(SourceDatabase source, TargetDatabase target) {
        this.source = source;
        this.target = target;
    }

    /**
     * migration
     */
    public abstract void migration();

    /**
     * sleep
     *
     * @param millis millis
     */
    protected void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            LOGGER.error("InterruptedException occurred while arranging packet files, error message is: {}.",
                e.getMessage());
        }
    }

    /**
     * getThreadFactory
     *
     * @param prefix prefix
     * @return ThreadFactory
     */
    protected ThreadFactory getThreadFactory(String prefix) {
        return new ThreadFactory() {
            private final AtomicInteger threadCount = new AtomicInteger(1);

            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName(prefix + threadCount.getAndIncrement());
                thread.setUncaughtExceptionHandler((t, e) -> {
                    LOGGER.error("Thread " + t.getName() + " threw an uncaught exception:");
                });
                return thread;
            }
        };
    }

    /**
     * waitThreadsTerminated
     *
     * @param threadPool threadPool
     * @param queueName queueName
     * @param isTaskFinish isTaskFinish
     */
    protected void waitThreadsTerminated(ThreadPoolExecutor threadPool, String queueName, boolean isTaskFinish) {
        threadPool.shutdown();
        while (true) {
            int activeCount = threadPool.getActiveCount();
            if (activeCount == 0) {
                process(queueName, isTaskFinish);
                break;
            }
            sleep(1000);
        }
    }

    private void process(String queueName, boolean isTaskFinish) {
        if (isTaskFinish) {
            if (source.isDumpJson()) {
                ProgressTracker.getInstance().setIsTaskStop(true);
            }
            LOGGER.info("table migration task has been completed.");
        } else {
            QueueManager.getInstance().setReadFinished(queueName, true);
            LOGGER.info("the {} has been consumed completely.", queueName);
        }
    }
}
