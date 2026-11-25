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

import org.full.migration.coordinator.QueueManager;
import org.full.migration.model.TaskTypeEnum;
import org.full.migration.source.SourceDatabase;
import org.full.migration.target.ITargetDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * KeyAndIndexMigration
 *
 * @since 2025-04-18
 */
public class KeyAndIndexMigration extends MigrationStrategy {
    private static final Logger LOGGER = LoggerFactory.getLogger(KeyAndIndexMigration.class);

    private final String type;

    /**
     * KeyAndIndexMigration
     *
     * @param source source
     * @param target target
     * @param type type
     */
    public KeyAndIndexMigration(SourceDatabase source, ITargetDatabase target, String type) {
        super(source, target);
        this.type = type;
    }

    @Override
    public void migration(String sourceDbType) {
        TableMigrationType tableMigrationType = TableMigrationType.fromType(type);
        int threadCount = 1;
        // Foreign key migration operations are strictly prohibited from concurrent execution.
        if(!tableMigrationType.getType().equalsIgnoreCase(TaskTypeEnum.FOREIGN_KEY.getTaskType())){
            threadCount = source.getSourceConfig().getWriterNum() + 1;
        }

        ThreadPoolExecutor executor = new ThreadPoolExecutor(threadCount, threadCount, 0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(), getThreadFactory("worker-"));
        Set<String> schemaSet = source.getSchemaSet();

        executor.submit(() -> tableMigrationType.getReadTask().accept(source, schemaSet));
        int writeCount = source.getSourceConfig().getWriterNum();
        for (int i = 0; i < writeCount; i++) {
            executor.submit(() -> {
                try {
                    tableMigrationType.getWriteTask().accept(target);
                } catch (Exception e) {
                    LOGGER.error("write {} has occurred exception.", tableMigrationType.getType(), e);
                }
            });
        }
        executor.shutdown();
        String queueName = "";
        if (tableMigrationType.getType().equalsIgnoreCase(TaskTypeEnum.PRIMARY_KEY.getTaskType())) {
            queueName = QueueManager.TABLE_PRIMARY_KEY_QUEUE;
        } else if (tableMigrationType.getType().equalsIgnoreCase(TaskTypeEnum.FOREIGN_KEY.getTaskType())) {
            queueName = QueueManager.TABLE_FOREIGN_KEY_QUEUE;
        } else if (tableMigrationType.getType().equalsIgnoreCase(TaskTypeEnum.CONSTRAINT.getTaskType())) {
            queueName = QueueManager.TABLE_CONSTRAINT_QUEUE;
        } else {
            queueName = QueueManager.TABLE_INDEX_QUEUE;
        }
        waitThreadsTerminated(executor, queueName, true);
    }
}
