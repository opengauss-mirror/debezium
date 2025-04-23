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

import org.apache.commons.lang3.StringUtils;
import org.full.migration.coordinator.QueueManager;
import org.full.migration.model.config.SourceConfig;
import org.full.migration.source.SourceDatabase;
import org.full.migration.target.TargetDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * TableMigration
 *
 * @since 2025-04-18
 */
public class TableMigration extends MigrationStrategy {
    private static final Logger LOGGER = LoggerFactory.getLogger(TableMigration.class);

    /**
     * TableMigration
     *
     * @param source source
     * @param target target
     */
    public TableMigration(SourceDatabase source, TargetDatabase target) {
        super(source, target);
    }

    @Override
    public void migration() {
        SourceConfig sourceConfig = source.getSourceConfig();
        // 1. 迁移schema
        target.createSchemas(new HashSet<>(sourceConfig.getSchemaMappings().values()));
        // 2. 迁移表
        int readerCount = sourceConfig.getReaderNum();
        int writeCount = sourceConfig.getWriterNum();
        ThreadPoolExecutor metaReadExecutor = getThreadPool("MetaReader-", readerCount);
        ThreadPoolExecutor metaWriteExecutor = getThreadPool("MetaWriter-", readerCount);
        ThreadPoolExecutor dataReadExecutor = getThreadPool("DataReader-", writeCount);
        ThreadPoolExecutor dataWriteExecutor = getThreadPool("DataWriter-", writeCount);
        Set<String> schemaSet = source.getSchemaSet();
        metaReadExecutor.submit(() -> source.queryTables(schemaSet));
        for (int i = 0; i < readerCount; i++) {
            metaReadExecutor.submit(source::readTableConstruct);
            dataReadExecutor.submit(source::readTable);
        }
        for (int i = 0; i < writeCount; i++) {
            metaWriteExecutor.submit(target::writeTableConstruct);
            dataWriteExecutor.submit(target::writeTable);
        }
        waitThreadsTerminated(metaReadExecutor, QueueManager.SOURCE_TABLE_META_QUEUE, false);
        waitThreadsTerminated(metaWriteExecutor, QueueManager.TARGET_TABLE_META_QUEUE, false);
        waitThreadsTerminated(dataReadExecutor, QueueManager.TABLE_DATA_QUEUE, false);
        waitThreadsTerminated(dataWriteExecutor, StringUtils.EMPTY, true);
    }

    private ThreadPoolExecutor getThreadPool(String prefix, int threadCount) {
        return new ThreadPoolExecutor(threadCount + 1, threadCount + 1, 0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(), getThreadFactory(prefix));
    }
}
