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
    public KeyAndIndexMigration(SourceDatabase source, TargetDatabase target, String type) {
        super(source, target);
        this.type = type;
    }

    @Override
    public void migration() {
        int threadCount = source.getSourceConfig().getWriterNum() + 1;
        ThreadPoolExecutor executor = new ThreadPoolExecutor(threadCount, threadCount, 0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(), getThreadFactory("worker-"));
        Set<String> schemaSet = source.getSchemaSet();
        TableMigrationType tableMigrationType = TableMigrationType.fromType(type);
        executor.submit(() -> tableMigrationType.getReadTask().accept(source, schemaSet));
        int writeCount = source.getSourceConfig().getWriterNum();
        for (int i = 0; i < writeCount; i++) {
            executor.submit(() -> tableMigrationType.getWriteTask().accept(target));
        }
        executor.shutdown();
    }
}
