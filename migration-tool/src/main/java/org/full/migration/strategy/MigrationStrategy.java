/*
 * Copyright 2026 Huawei Technologies Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.full.migration.strategy;

import org.full.migration.coordinator.ProgressTracker;
import org.full.migration.coordinator.QueueManager;
import org.full.migration.exception.MigrationException;
import org.full.migration.source.SourceDatabase;
import org.full.migration.target.ITargetDatabase;
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
    protected final ITargetDatabase target;

    /**
     * MigrationStrategy
     *
     * @param source source
     * @param target target
     */
    public MigrationStrategy(SourceDatabase source, ITargetDatabase target) {
        this.source = source;
        this.target = target;
    }

    /**
     * migration
     * @throws MigrationException 
     */
    public abstract void migration(String sourceDbType) throws MigrationException;

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
            if (queueName.equalsIgnoreCase(QueueManager.TABLE_CONSTRAINT_QUEUE)) {
                LOGGER.info("constraint migration complete. full report thread is close.");
            } else {
                LOGGER.info("table migration task has been completed.");
            }
        } else {
            QueueManager.getInstance().setReadFinished(queueName, true);
            LOGGER.info("the {} has been consumed completely.", queueName);
        }
    }
}
