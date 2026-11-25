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

import org.apache.commons.lang3.StringUtils;
import org.full.migration.coordinator.QueueManager;
import org.full.migration.exception.MigrationException;
import org.full.migration.model.PostgresCustomTypeMeta;
import org.full.migration.model.config.SourceConfig;
import org.full.migration.source.SourceDatabase;
import org.full.migration.target.ITargetDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
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
    private static final Logger logger = LoggerFactory.getLogger(TableMigration.class);

    /**
     * TableMigration
     *
     * @param source source
     * @param target target
     */
    public TableMigration(SourceDatabase source, ITargetDatabase target) {
        super(source, target);
    }

    @Override
    public void migration(String sourceDbType) throws MigrationException {
        if (source == null) {
            throw new IllegalStateException("Source is not initialized.");
        }
        SourceConfig sourceConfig = source.getSourceConfig();
        // 1. 迁移schema
        target.createSchemas(new HashSet<>(sourceConfig.getSchemaMappings().values()));
        // 2. 迁移表
        int readerCount = sourceConfig.getReaderNum();
        logger.info("readerCount is: {}", readerCount);
        int writeCount = sourceConfig.getWriterNum();
        logger.info("writeCount is: {}", writeCount);
        ThreadPoolExecutor metaReadExecutor = getThreadPool("MetaReader-", readerCount);
        ThreadPoolExecutor metaWriteExecutor = getThreadPool("MetaWriter-", readerCount);
        ThreadPoolExecutor dataReadExecutor = getThreadPool("DataReader-", writeCount);
        ThreadPoolExecutor dataWriteExecutor = getThreadPool("DataWriter-", writeCount);
        Set<String> schemaSet = source.getSchemaSet();
        source.initializeLogicalReplication(schemaSet);
        List<PostgresCustomTypeMeta> customTypes = source.queryCustomOrDomainTypes(schemaSet);
        target.createCustomOrDomainTypes(customTypes);
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
