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

package org.full.migration.target;

import org.full.migration.enums.SqlCompatibilityEnum;
import org.full.migration.exception.MigrationException;
import org.full.migration.model.config.DatabaseConfig;
import org.full.migration.model.config.GlobalConfig;
import org.full.migration.utils.OpenGaussUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

/**
 * TargetDatabaseFactory
 * Target database factory class for creating different types of target database implementations
 *
 * @since 2025-04-18
 */
public class TargetDatabaseFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(TargetDatabaseFactory.class);

    /**
     * getTargetDatabase
     * Create appropriate target database implementation based on source and target database types
     *
     * @param sourceDbType Source database type
     * @param targetDbType Target database type
     * @param globalConfig Global configuration
     * @return Target database implementation
     * @throws MigrationException If DataX initialization fails
     */
    public static ITargetDatabase getTargetDatabase(String sourceDbType, String targetDbType,
                                                    GlobalConfig globalConfig) throws MigrationException {
        LOGGER.info("Creating target database for source: {}, target: {}", sourceDbType, targetDbType);
        // Check if DataX mode is needed
        if (isOracle2OgracScenario(sourceDbType, targetDbType)) {
            LOGGER.info("Using OgracTargetDatabase for Oracle2Ograc scenario");
            return new OgracTargetDatabase(globalConfig);
        } else {
            LOGGER.info("Using traditional TargetDatabase for CSV scenario");
            return new TargetDatabase(globalConfig, getOpenGaussSqlCompatibilityEnum(globalConfig.getOgConn()));
        }
    }
    
    /**
     * isOracle2OgracScenario
     * Determine if it's a Oracle2Ograc scenario
     *
     * @param sourceDbType Source database type
     * @param targetDbType Target database type
     * @return Whether it's a DataX scenario
     */
    private static boolean isOracle2OgracScenario(String sourceDbType, String targetDbType) {
        // Define scenarios that require DataX
        // 1. Oracle to Ograc migration
        if ("oracle".equalsIgnoreCase(sourceDbType) && "ograc".equalsIgnoreCase(targetDbType)) {
            return true;
        }
        // Default to traditional CSV mode
        return false;
    }

    private static SqlCompatibilityEnum getOpenGaussSqlCompatibilityEnum(DatabaseConfig databaseConfig) {
        try {
            return OpenGaussUtils.getSqlCompatibilityEnum(databaseConfig);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to get sql_compatibility from openGauss target", e);
        }
    }
}
