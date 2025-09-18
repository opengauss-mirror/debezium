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

package org.full.migration.coordinator;

import org.full.migration.YAMLLoader;
import org.full.migration.constants.MigrationConfigConstants;
import org.full.migration.model.TaskTypeEnum;
import org.full.migration.model.config.GlobalConfig;
import org.full.migration.source.SourceDatabase;
import org.full.migration.source.SourceDatabaseFactory;
import org.full.migration.strategy.MigrationStrategy;
import org.full.migration.strategy.StrategyFactory;
import org.full.migration.target.TargetDatabase;
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
        Optional<GlobalConfig> globalConfigOptional = YAMLLoader.loadYamlConfig(configPath);
        if (!globalConfigOptional.isPresent()) {
            return;
        }
        GlobalConfig globalConfig = globalConfigOptional.get();
        if (!globalConfig.getSourceConfig().isValid(taskType)) {
            return;
        }
        getDatabasePasswordFromEnv(globalConfig);
        SourceDatabaseFactory.buildStrategyMap(globalConfig);
        SourceDatabase source = SourceDatabaseFactory.getSourceDatabase(sourceDbType);
        TargetDatabase target = new TargetDatabase(globalConfig);
        StrategyFactory.buildStrategyMap(source, target);
        MigrationStrategy strategy = StrategyFactory.getMigrationStrategy(taskType);
        if (strategy == null) {
            LOGGER.error("--start parameter is invalid, please modify and retry");
            return;
        }
        if (globalConfig.getIsDumpJson()) {
            String statusDir = globalConfig.getStatusDir().replace("~", System.getProperty("user.home"));
            ProgressTracker.initInstance(statusDir, taskType);
        }
        strategy.migration(sourceDbType);
    }

    private void getDatabasePasswordFromEnv(GlobalConfig globalConfig) {
        String isEnableStdinPassword = System.getenv(MigrationConfigConstants.ENABLE_ENV_PASSWORD);
        if (isEnableStdinPassword != null && isEnableStdinPassword.equals("true")) {
            String openGaussPassword = System.getenv(MigrationConfigConstants.OPENGAUSS_PASSWORD);
            if (openGaussPassword != null) {
                globalConfig.getOgConn().setPassword(openGaussPassword);
            }
            String sourceDbPassword = System.getenv(MigrationConfigConstants.SOURCE_DB_PASSWORD);
            if (sourceDbPassword != null) {
                globalConfig.getSourceConfig().getDbConn().setPassword(sourceDbPassword);
            }
        }
    }
}
