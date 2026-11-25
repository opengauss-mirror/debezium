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

import org.full.migration.model.config.DatabaseConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Locale;

/**
 * SqlServerConnection
 *
 * @since 2025-04-18
 */
public class SqlServerConnection implements JdbcConnection {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerConnection.class);
    private static final String SQLSERVER_URL = "jdbc:sqlserver://%s:%d;databaseName=%s;encrypt=false;loginTimeout=0";

    @Override
    public Connection getConnection(DatabaseConfig dbConfig) throws SQLException {
        String url = String.format(Locale.ROOT, SQLSERVER_URL, dbConfig.getHost(), dbConfig.getPort(),
            dbConfig.getDatabase());
        try {
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            return DriverManager.getConnection(url, dbConfig.getUser(), dbConfig.getPassword());
        } catch (SQLException | ClassNotFoundException e) {
            LOGGER.error("fail to create sql server connection, host:{}, port:{}, please check.", dbConfig.getHost(),
                dbConfig.getPort());
            throw new SQLException(e.getMessage());
        }
    }

    @Override
    public Connection retryConnection(DatabaseConfig dbConfig) throws SQLException {
        String url = String.format(Locale.ROOT, SQLSERVER_URL, dbConfig.getHost(), dbConfig.getPort(),
            dbConfig.getDatabase());
        Connection connection = null;
        int tryCount = 0;
        while (connection == null && tryCount < RETRY_TIME) {
            try {
                Thread.sleep(SLEEP_TIME);
                LOGGER.info("try re-connect ing");
                connection = DriverManager.getConnection(url, dbConfig.getUser(), dbConfig.getPassword());
            } catch (SQLException | InterruptedException e) {
                LOGGER.error("Unable to connect to database {}:{}, error message is: {}", dbConfig.getHost(),
                    dbConfig.getPort(), e.getMessage());
            }
            tryCount++;
        }
        return connection;
    }
}
