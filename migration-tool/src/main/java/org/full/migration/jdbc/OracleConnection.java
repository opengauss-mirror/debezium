/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

package org.full.migration.jdbc;

import org.full.migration.exception.DatabaseConnectionException;
import org.full.migration.exception.ErrorCode;
import org.full.migration.model.config.DatabaseConfig;
import org.full.migration.utils.JdbcUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * OpenGaussConnection
 *
 * @since 2025-04-18
 */
public class OracleConnection implements JdbcConnection {
    private static final Logger LOGGER = LoggerFactory.getLogger(OracleConnection.class);
    private static final int RETRY_TIME = 3;
    private static final long SLEEP_TIME = 3000;
    
    static {
        try {
            Class.forName("oracle.jdbc.OracleDriver");
            LOGGER.info("Oracle driver loaded successfully");
        } catch (ClassNotFoundException e) {
            LOGGER.error("Oracle driver not found: {}", e.getMessage());
            throw new ExceptionInInitializerError(new DatabaseConnectionException(ErrorCode.DRIVER_LOAD_FAILED.getCode(), "Failed to load Oracle driver", e));
        }
    }
    
    @Override
    public Connection getConnection(DatabaseConfig dbConfig) throws SQLException {
        String sourceUrl = JdbcUtils.generateOracleJdbcUrl(dbConfig);
        try {
            Connection conn = DriverManager.getConnection(sourceUrl, dbConfig.getUser(), dbConfig.getPassword());
            if (conn != null && !conn.isClosed()) {
                conn.setAutoCommit(false);
                return conn;
            } else {
                throw new SQLException("Failed to get valid connection");
            }
        } catch (SQLException e) {
            LOGGER.error("Unable to connect to database {}:{}, error message is: {}", dbConfig.getHost(),
                dbConfig.getPort(), e.getMessage());
            throw e;
        }
    }

    @Override
    public Connection retryConnection(DatabaseConfig dbConfig) throws SQLException {
        String sourceUrl = JdbcUtils.generateOracleJdbcUrl(dbConfig);;
        Connection connection = null;
        int tryCount = 0;
        while (connection == null && tryCount < RETRY_TIME) {
            try {
                Thread.sleep(SLEEP_TIME);
                LOGGER.info("try re-connect ing");
                connection = DriverManager.getConnection(sourceUrl, dbConfig.getUser(), dbConfig.getPassword());
                if (connection != null && !connection.isClosed()) {
                    connection.setAutoCommit(false);
                } else {
                    connection = null;
                }
            } catch (SQLException | InterruptedException e) {
                LOGGER.error("Unable to connect to database {}:{}, error message is: {}", dbConfig.getHost(),
                    dbConfig.getPort(), e.getMessage());
                connection = null;
            }
            tryCount++;
        }
        return connection;
    }
}
