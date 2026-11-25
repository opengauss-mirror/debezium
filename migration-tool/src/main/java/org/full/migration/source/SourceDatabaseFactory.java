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

package org.full.migration.source;

import org.full.migration.enums.SqlCompatibilityEnum;
import org.full.migration.model.config.GlobalConfig;
import org.full.migration.source.opengauss.OpenGaussASource;
import org.full.migration.source.opengauss.OpenGaussBSource;
import org.full.migration.source.opengauss.OpenGaussSource;
import org.full.migration.utils.OpenGaussUtils;

import java.sql.SQLException;

/**
 * SourceDatabaseFactory
 *
 * @since 2025-04-18
 */
public class SourceDatabaseFactory {
    /**
     * getSourceDatabase
     *
     * @param dbType dbType
     * @return SourceDatabase
     */
    public static SourceDatabase getSourceDatabase(GlobalConfig globalConfig, String dbType) {
        if ("sqlserver".equals(dbType)) {
            return new SqlServerSource(globalConfig);
        } else if ("postgresql".equals(dbType)) {
            return new PostgresSource(globalConfig);
        } else if ("opengauss".equals(dbType)) {
            return createOpenGaussSource(globalConfig);
        } else if ("oracle".equals(dbType)) {
            return new OracleSourceDatabase(globalConfig);
        } else {
            throw new IllegalArgumentException("Unsupported source database type: " + dbType);
        }
    }

    private static OpenGaussSource createOpenGaussSource(GlobalConfig globalConfig) {
        try {
            SqlCompatibilityEnum compatibilityEnum = OpenGaussUtils.getSqlCompatibilityEnum(
                    globalConfig.getSourceConfig().getDbConn());
            if (compatibilityEnum.equals(SqlCompatibilityEnum.A)) {
                return new OpenGaussASource(globalConfig);
            } else if (compatibilityEnum.equals(SqlCompatibilityEnum.B)) {
                return new OpenGaussBSource(globalConfig);
            } else {
                throw new RuntimeException("Unsupported source openGauss sql_compatibility: " + compatibilityEnum);
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to get sql_compatibility from openGauss source", e);
        }
    }
}
