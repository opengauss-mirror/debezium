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

import org.full.migration.source.SourceDatabase;
import org.full.migration.target.TargetDatabase;
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
    public DropReplicaSchema(SourceDatabase source, TargetDatabase target) {
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
