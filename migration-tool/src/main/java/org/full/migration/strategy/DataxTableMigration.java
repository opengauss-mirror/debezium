/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

package org.full.migration.strategy;

import org.full.migration.coordinator.QueueManager;
import org.full.migration.datax.DataXInstall;
import org.full.migration.exception.DataXMigrationException;
import org.full.migration.exception.MigrationException;
import org.full.migration.model.PostgresCustomTypeMeta;
import org.full.migration.model.config.SourceConfig;
import org.full.migration.source.SourceDatabase;
import org.full.migration.target.ITargetDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * DataxTableMigration
 * Table migration strategy based on DataX, 
 * specifically designed for migration scenarios that require DataX, such as Oracle to oGRAC
 *
 * @since 2025-04-18
 */
public class DataxTableMigration extends MigrationStrategy {
    private static final Logger logger = LoggerFactory.getLogger(DataxTableMigration.class);

    private Integer threadQueueCapacity;

    /**
     * DataxTableMigration
     *
     * @param source source
     * @param target target
     */
    public DataxTableMigration(SourceDatabase source, ITargetDatabase target) {
        super(source, target);
        try {
            this.threadQueueCapacity = source.getSourceConfig().getThreadQueueCapacity();
            logger.info("Using threadQueueCapacity from sourceConfig: {}", threadQueueCapacity);
        } catch (Exception e) {
            this.threadQueueCapacity = null;
            logger.warn("Failed to get threadQueueCapacity from sourceConfig: {}", e.getMessage());
        }
    }

    @Override
    public void migration(String sourceDbType) throws MigrationException {
        logger.info("Starting DataX-based table migration for source type: {}", sourceDbType);
        
        if (source == null) {
            throw new IllegalStateException("Source is not initialized.");
        }
        SourceConfig sourceConfig = source.getSourceConfig();
        
        int metaThreadCount = sourceConfig.getReaderNum();
        int dataThreadCount = sourceConfig.getWriterNum();
        logger.info("metaThreadCount is: {}", metaThreadCount);
        logger.info("dataThreadCount is: {}", dataThreadCount);
        
        ThreadPoolExecutor metaReadExecutor = getThreadPool("DataXMetaReader-", metaThreadCount);
        ThreadPoolExecutor metaWriteExecutor = getThreadPool("DataXMetaWriter-", metaThreadCount);
        ThreadPoolExecutor dataMigrationExecutor = getThreadPool("DataXMigration-", dataThreadCount);
        
        Set<String> schemaSet = source.getSchemaSet();
        logger.info("Migrating schemas: {}", schemaSet);
        
        List<PostgresCustomTypeMeta> customTypes = source.queryCustomOrDomainTypes(schemaSet);
        target.createCustomOrDomainTypes(customTypes);
        
        // Submit all tasks first (producers and consumers run concurrently),
        // then wait for completion in dependency order — same pattern as TableMigration
        logger.info("Starting metadata reading phase");
        metaReadExecutor.submit(() -> {
            try {
                logger.info("Starting to query tables from source database");
                source.queryTables(schemaSet);
                logger.info("Table query completed");
            } catch (Exception e) {
                logger.error("Error querying tables: {}", e.getMessage());
            }
        });
        
        for (int i = 0; i < metaThreadCount; i++) {
            metaReadExecutor.submit(() -> {
                try {
                    logger.info("Starting table construct reader thread");
                    source.readTableConstruct();
                    logger.info("Table construct reader thread completed");
                } catch (Exception e) {
                    logger.error("Error reading table construct: {}", e.getMessage());
                }
            });
        }
        
        logger.info("Starting metadata writing phase");
        for (int i = 0; i < metaThreadCount; i++) {
            metaWriteExecutor.submit(() -> {
                try {
                    logger.info("Starting table construct writer thread");
                    target.writeTableConstruct();
                    logger.info("Table construct writer thread completed");
                } catch (Exception e) {
                    logger.error("Error writing table construct: {}", e.getMessage());
                }
            });
        }
        
        logger.info("Starting data migration phase with DataX");
        for (int i = 0; i < dataThreadCount; i++) {
            dataMigrationExecutor.submit(() -> {
                try {
                    logger.info("Starting DataX migration thread");
                    target.writeTable();
                    logger.info("DataX migration thread completed");
                } catch (Exception e) {
                    logger.error("Error executing DataX migration: {}", e.getMessage());
                }
            });
        }
        
        // Wait for all phases in dependency order:
        // 1. metaReadExecutor finishes → SOURCE_TABLE_META_QUEUE.readFinished = true
        //    → writeTableConstruct() consumers can exit their poll loop
        // 2. metaWriteExecutor finishes → TARGET_TABLE_META_QUEUE.readFinished = true
        //    → writeTable() DataX consumers can exit their poll loop
        // 3. dataMigrationExecutor finishes → migration complete
        logger.info("Waiting for metadata reading threads to complete");
        waitThreadsTerminated(metaReadExecutor, QueueManager.SOURCE_TABLE_META_QUEUE, false);
        logger.info("Waiting for metadata writing threads to complete");
        waitThreadsTerminated(metaWriteExecutor, QueueManager.TARGET_TABLE_META_QUEUE, false);
        logger.info("Waiting for DataX migration threads to complete");
        waitThreadsTerminated(dataMigrationExecutor, "", true);
        logger.info("DataX-based table migration completed successfully");
    }

    public void initializedDataXTools(String dataxHome) throws DataXMigrationException {
        System.setProperty("DataX_HOME", dataxHome);
        DataXInstall.initializeDataXTools(dataxHome);
    }

    protected ThreadPoolExecutor getThreadPool(String prefix, int threadCount) {
        int queueCapacity;
        if (threadQueueCapacity != null && threadQueueCapacity > 0) {
            queueCapacity = threadQueueCapacity;
        } else {
            queueCapacity = 2000; 
        }
        
        logger.info("Creating thread pool {} with queue capacity: {}", prefix, queueCapacity);
        RejectedExecutionHandler handler = new ThreadPoolExecutor.CallerRunsPolicy();
        return new ThreadPoolExecutor(
            threadCount,
            threadCount,
            0L,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(queueCapacity),
            getThreadFactory(prefix),
            handler
        );
    }
}
