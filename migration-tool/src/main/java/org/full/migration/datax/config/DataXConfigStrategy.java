/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

package org.full.migration.datax.config;

import org.full.migration.datax.model.DataXConfig;
import org.full.migration.exception.DataXMigrationException;

/**
 * DataXConfigStrategy
 * DataX configuration generation strategy interface
 *
 * @since 2025-04-18
 */
public interface DataXConfigStrategy {
    /**
     * Generate DataX configuration object
     *
     * @param context Configuration context
     * @return DataXConfig configuration object
     */
    DataXConfig generateConfig(DataXConfigContext context) throws DataXMigrationException;
    
    /**
     * Get the name of the strategy
     *
     * @return Strategy name
     */
    String getStrategyName();
    
    /**
     * Get the JVM parameters required by this strategy
     * @param rowCount Row count of the table to be migrated
     * @return JVM parameter string
     */
    String getJvmParameters(long rowCount);
}