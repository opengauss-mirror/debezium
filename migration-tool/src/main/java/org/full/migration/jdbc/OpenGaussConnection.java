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

package org.full.migration.jdbc;

import org.full.migration.model.config.DatabaseConfig;
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

    @Override
    public Connection getConnection(DatabaseConfig dbConfig) throws SQLException {
        String sourceUrl = String.format(Locale.ROOT, JDBC_URL, dbConfig.getHost(), dbConfig.getPort(),
            dbConfig.getDatabase(), dbConfig.getSchema());
        try {
            return DriverManager.getConnection(sourceUrl, dbConfig.getUser(), dbConfig.getPassword());
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
