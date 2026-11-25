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

package org.full.migration.jdbc;

import org.full.migration.enums.SqlCompatibilityEnum;
import org.full.migration.model.config.DatabaseConfig;
import org.full.migration.utils.OpenGaussUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Locale;

/**
 * OpenGaussConnection
 *
 * @since 2025-04-18
 */
public class OpenGaussConnection implements JdbcConnection {
    private static final Logger LOGGER = LoggerFactory.getLogger(OpenGaussConnection.class);
    private static final String JDBC_URL
        = "jdbc:opengauss://%s:%s/%s?currentSchema=%s&connectTimeout=10&loggerLevel=error";

    private final SqlCompatibilityEnum sqlCompatibility;

    public OpenGaussConnection() {
        sqlCompatibility = SqlCompatibilityEnum.A;
    }

    public OpenGaussConnection(SqlCompatibilityEnum sqlCompatibility) {
        this.sqlCompatibility = sqlCompatibility;
    }

    @Override
    public Connection getConnection(DatabaseConfig dbConfig) throws SQLException {
        String sourceUrl = String.format(Locale.ROOT, JDBC_URL, dbConfig.getHost(), dbConfig.getPort(),
            dbConfig.getDatabase(), dbConfig.getSchema());
        try {
            Connection connection = DriverManager.getConnection(sourceUrl, dbConfig.getUser(), dbConfig.getPassword());
            if (sqlCompatibility.equals(SqlCompatibilityEnum.B)) {
                OpenGaussUtils.openDolphinSqlModeAnsiQuotes(connection);
            }
            return connection;
        } catch (SQLException e) {
            LOGGER.error("Unable to connect to database {}:{}, error message is: {}", dbConfig.getHost(),
                dbConfig.getPort(), e.getMessage());
            throw e;
        }
    }

    @Override
    public Connection retryConnection(DatabaseConfig dbConfig) throws SQLException {
        String sourceUrl = String.format(Locale.ROOT, JDBC_URL, dbConfig.getHost(), dbConfig.getPort(),
            dbConfig.getDatabase(), dbConfig.getSchema());
        Connection connection = null;
        int tryCount = 0;
        while (connection == null && tryCount < RETRY_TIME) {
            try {
                Thread.sleep(SLEEP_TIME);
                LOGGER.info("try re-connect ing");
                connection = DriverManager.getConnection(sourceUrl, dbConfig.getUser(), dbConfig.getPassword());
            } catch (SQLException | InterruptedException e) {
                LOGGER.error("Unable to connect to database {}:{}, error message is: {}", dbConfig.getHost(),
                    dbConfig.getPort(), e.getMessage());
            }
            tryCount++;
        }
        return connection;
    }
}
