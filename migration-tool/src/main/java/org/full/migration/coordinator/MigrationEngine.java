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

package org.full.migration.coordinator;

import org.full.migration.YAMLLoader;
import org.full.migration.constants.MigrationConfigConstants;
import org.full.migration.coordinator.check.ScenarioCheckStrategyFactory;
import org.full.migration.exception.ErrorCode;
import org.full.migration.exception.MigrationException;
import org.full.migration.model.config.GlobalConfig;
import org.full.migration.source.SourceDatabase;
import org.full.migration.source.SourceDatabaseFactory;
import org.full.migration.strategy.DataxTableMigration;
import org.full.migration.strategy.MigrationStrategy;
import org.full.migration.strategy.StrategyFactory;
import org.full.migration.target.ITargetDatabase;
import org.full.migration.target.TargetDatabaseFactory;
import org.full.migration.utils.LogUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;
import java.util.Optional;

/**
 * MigrationEngine
 *
 * @since 2025-04-18
 */
public class MigrationEngine {
    private static final Logger LOGGER = LoggerFactory.getLogger(MigrationEngine.class);

    private final String taskType;
    private final String sourceDbType;
    private final String configPath;

    /**
     * MigrationEngine
     *
     * @param taskType taskType
     * @param sourceDbType sourceDbType
     * @param configPath configPath
     */
    public MigrationEngine(String taskType, String sourceDbType, String configPath) {
        this.taskType = taskType.toLowerCase(Locale.ROOT);
        this.sourceDbType = sourceDbType.toLowerCase(Locale.ROOT);
        this.configPath = configPath;
    }

    /**
     * dispatch
     */
    public void dispatch() {
        try {
            Optional<GlobalConfig> globalConfigOptional = YAMLLoader.loadYamlConfig(configPath);
            if (!globalConfigOptional.isPresent()) {
                LOGGER.error("Failed to load configuration file: {}", configPath);
                return;
            }
            GlobalConfig globalConfig = globalConfigOptional.get();
            if (!ScenarioCheckStrategyFactory.getStrategy(sourceDbType).check(globalConfig, taskType)) {
                LOGGER.error("Invalid configuration for task type: {} and source database: {}", taskType, sourceDbType);
                return;
            }
            getDatabasePasswordFromEnv(globalConfig);

            SourceDatabase source = SourceDatabaseFactory.getSourceDatabase(globalConfig, sourceDbType);
            if (source == null) {
                LOGGER.error("Failed to create source database instance");
                return;
            }
            source.checkConnection();
            String targetDbType = getTargetDatabaseType(globalConfig);
            LOGGER.info("Migration scenario: source={}, target={}", sourceDbType, targetDbType);

            ITargetDatabase target = null;
            try {
                target = TargetDatabaseFactory.getTargetDatabase(sourceDbType, targetDbType, globalConfig);
                if (target == null) {
                    LOGGER.error("Failed to create target database instance");
                    return;
                }
                target.checkConnection();
                StrategyFactory.buildStrategyMap(source, target);
                MigrationStrategy strategy = StrategyFactory.getMigrationStrategy(taskType);
                if (strategy == null) {
                    LOGGER.error("--start parameter is invalid, please modify and retry");
                    return;
                }
                if(strategy instanceof DataxTableMigration dataxTableMigration) {
                    dataxTableMigration.initializedDataXTools(globalConfig.getDatax().getDataxHome());
                }
                if (globalConfig.getIsDumpJson()) {
                    String statusDir = globalConfig.getStatusDir().replace("~", System.getProperty("user.home"));
                    ProgressTracker.initInstance(statusDir, taskType);
                }
                strategy.migration(sourceDbType);
            }  catch (Exception e) {
                handleGeneralException(e);
            } finally {
                if (target != null) {
                    try {
                        target.shutdown();
                    } catch (Exception e) {
                        LOGGER.warn("Error during target database shutdown: {}", e.getMessage());
                    }
                }
            }
        } catch (Exception e) {
            handleGeneralException(e);
        }
    }
    
    /**
     * Handle migration exceptions
     * @param e Migration exception
     */
    private void handleMigrationException(MigrationException e) {
        LogUtils.logException(e);
    }
    
    /**
     * Handle general exceptions
     * @param e General exception
     */
    private void handleGeneralException(Exception e) {
        if(e instanceof MigrationException) {
            handleMigrationException((MigrationException) e);
            return;
        }else{
            // Wrap other exceptions as general migration exceptions
            MigrationException migrationException = new MigrationException(
                    ErrorCode.UNKNOWN_ERROR.getCode(),
                    ErrorCode.UNKNOWN_ERROR.getMessage(),
                    e.getMessage(),
                    e
            );
            handleMigrationException(migrationException);
        }
    }

    /**
     * getTargetDatabaseType
     * Get the target database type from the global configuration.
     * If no target type is specified in the configuration, the default value "opengauss" is used.
     *
     * @param globalConfig Global configuration
     * @return Target database type
     */
    private String getTargetDatabaseType(GlobalConfig globalConfig) {
        // Read the target database type from the configuration.
        // If no target type is specified, use the default value "opengauss".
        if (globalConfig.getTargetType() != null && !globalConfig.getTargetType().isEmpty()) {
            return globalConfig.getTargetType().toLowerCase(Locale.ROOT);
        }
        return "opengauss";
    }

    private void getDatabasePasswordFromEnv(GlobalConfig globalConfig) {
        String isEnableStdinPassword = System.getenv(MigrationConfigConstants.ENABLE_ENV_PASSWORD);
        if (isEnableStdinPassword != null && isEnableStdinPassword.equals("true")) {
            String openGaussPassword = System.getenv(MigrationConfigConstants.OPENGAUSS_PASSWORD);
            if (openGaussPassword != null) {
                if (globalConfig.getOgConn() != null) {
                    globalConfig.getOgConn().setPassword(openGaussPassword);
                }
            }
            String sourceDbPassword = System.getenv(MigrationConfigConstants.SOURCE_DB_PASSWORD);
            if (sourceDbPassword != null) {
                globalConfig.getSourceConfig().getDbConn().setPassword(sourceDbPassword);
            }
        }
    }
}
