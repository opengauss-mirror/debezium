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
