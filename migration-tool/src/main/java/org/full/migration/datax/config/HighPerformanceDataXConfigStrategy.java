/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

package org.full.migration.datax.config;

import org.full.migration.datax.model.WriterParameter;

public class HighPerformanceDataXConfigStrategy extends GeneralDataXConfigStrategy {

    private static final String STRATEGY_NAME = "high_performance_datax_strategy";

    private static final int BATCH_MULTIPLIER = 3;
    private static final int MIN_CHANNELS = 1;
    private static final int MAX_CHANNELS_PER_TABLE = 16;

    private static final long SMALL_TABLE_THRESHOLD = 10000;
    private static final long MEDIUM_TABLE_THRESHOLD = 100000;
    private static final long LARGE_TABLE_THRESHOLD = 1000000;

    @Override
    public String getStrategyName() {
        return STRATEGY_NAME;
    }

    @Override
    protected int getChannelCount(long rowCount, DataXCommonConfig commonConfig) {
        int availableCores = Runtime.getRuntime().availableProcessors();
        int recommended = calculateChannelCount(rowCount, availableCores);
        
        int finalChannels = Math.min(recommended, MAX_CHANNELS_PER_TABLE);
        
        if (commonConfig != null && commonConfig.getMaxChannels() > 0) {
            finalChannels = Math.min(finalChannels, commonConfig.getMaxChannels());
        }
        
        return Math.max(MIN_CHANNELS, finalChannels);
    }

    private int calculateChannelCount(long rowCount, int availableCores) {
        if (rowCount <= SMALL_TABLE_THRESHOLD) {
            return Math.min(4, availableCores / 4);
        }
        if (rowCount <= MEDIUM_TABLE_THRESHOLD) {
            return Math.min(8, availableCores / 2);
        }
        if (rowCount <= LARGE_TABLE_THRESHOLD) {
            return Math.min(12, availableCores);
        }
        return Math.min(MAX_CHANNELS_PER_TABLE, availableCores);
    }

    @Override
    public String getJvmParameters(long rowCount) {
        long maxMemoryGB = Runtime.getRuntime().maxMemory() / (1024 * 1024 * 1024);
        
        if (rowCount <= SMALL_TABLE_THRESHOLD) {
            return "-Xms2g -Xmx2g";
        }
        if (rowCount <= LARGE_TABLE_THRESHOLD) {
            return "-Xms4g -Xmx4g";
        }
        if (rowCount <= 10 * LARGE_TABLE_THRESHOLD) {
            return "-Xms8g -Xmx8g";
        }
        
        if (maxMemoryGB >= 32) {
            return "-Xms16g -Xmx16g -XX:+UseG1GC -XX:MaxGCPauseMillis=200";
        }
        if (maxMemoryGB >= 16) {
            return "-Xms8g -Xmx8g -XX:+UseG1GC -XX:MaxGCPauseMillis=200";
        }
        return "-Xms4g -Xmx4g -XX:+UseG1GC";
    }

    @Override
    protected int getReaderBatchSize(long rowCount) {
        return super.getReaderBatchSize(rowCount) * BATCH_MULTIPLIER;
    }

    @Override
    protected int getWriterBatchSize(long rowCount) {
        return super.getWriterBatchSize(rowCount) * BATCH_MULTIPLIER;
    }

    @Override
    protected int getBatchInsertSize(long rowCount) {
        return super.getBatchInsertSize(rowCount) * BATCH_MULTIPLIER;
    }

    @Override
    protected void configurePreAndPostSql(WriterParameter writerParam, String targetSchemaName, String tableName) {
        super.configurePreAndPostSql(writerParam, targetSchemaName, tableName);
        writerParam.addPreSql("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'");
        writerParam.addPreSql("ALTER SESSION SET NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF'");
    }
}