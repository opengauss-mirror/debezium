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

import org.full.migration.coordinator.QueueManager;
import org.full.migration.source.SourceDatabase;
import org.full.migration.target.TargetDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * ObjectMigration
 *
 * @since 2025-04-18
 */
public class ObjectMigration extends MigrationStrategy {
    private static final Logger LOGGER = LoggerFactory.getLogger(ObjectMigration.class);
    private final String objectType;

    /**
     * ObjectMigration
     *
     * @param source source
     * @param target target
     * @param objectType objectType
     */
    public ObjectMigration(SourceDatabase source, TargetDatabase target, String objectType) {
        super(source, target);
        this.objectType = objectType;
    }

    @Override
    public void migration() {
        int threadCount = source.getSourceConfig().getWriterNum() + 1;
        ThreadPoolExecutor executor = new ThreadPoolExecutor(threadCount, threadCount, 0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(), getThreadFactory("worker-"));
        Set<String> schemaSet = source.getSchemaSet();
        try {
            for (String schema : schemaSet) {
                executor.submit(() -> source.readObjects(objectType, schema));
                executor.submit(() -> target.writeObjects(objectType, schema));
            }
        } catch (IllegalArgumentException e) {
            LOGGER.error(e.getMessage());
        }
        executor.shutdown();
        waitThreadsTerminated(executor, QueueManager.OBJECT_QUEUE, true);
    }
}
