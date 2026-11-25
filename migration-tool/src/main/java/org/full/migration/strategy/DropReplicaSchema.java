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

import org.full.migration.source.SourceDatabase;
import org.full.migration.target.ITargetDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * DropReplicaSchema
 *
 * @since 2025-04-18
 */
public class DropReplicaSchema extends MigrationStrategy {
    private static final Logger LOGGER = LoggerFactory.getLogger(DropReplicaSchema.class);

    /**
     * ObjectMigration
     *
     * @param source source
     * @param target target
     */
    public DropReplicaSchema(SourceDatabase source, ITargetDatabase target) {
        super(source, target);
    }

    @Override
    public void migration(String sourceDbType) {
        int threadCount = source.getSourceConfig().getWriterNum() + 1;
        ThreadPoolExecutor executor = new ThreadPoolExecutor(threadCount, threadCount, 0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(), getThreadFactory("worker-"));
        Set<String> schemaSet = source.getSchemaSet();
        try {
            for (String schema : schemaSet) {
                executor.submit(() -> target.dropReplicaSchema());
            }
        } catch (IllegalArgumentException e) {
            LOGGER.error(e.getMessage());
        }
        executor.shutdown();
    }
}
