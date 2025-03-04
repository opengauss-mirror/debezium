/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package io.debezium.connector.postgresql.sink.object;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;

import io.debezium.enums.ErrorCode;
import io.debezium.util.MigrationProcessController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.postgresql.sink.task.PostgresSinkConnectorConfig;

/**
 * ConnectionInfo class
 *
 * @author tianbin
 * @since 2024-11-25
 */
public class ConnectionInfo {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionInfo.class);

    /**
     * The openGauss JDBC driver class
     */
    private static final String OPENGAUSS_JDBC_DRIVER = "org.opengauss.Driver";

    private static final int DEFAULT_RECONNECT_INTERVAL = 5;

    private Integer port;
    private final String username;
    private final String password;
    private final String ip;
    private final AtomicBoolean isConnectionAlive;
    private String database;
    private long waitTimeoutSecond;
    private int reconnectInterval;

    /**
     * Constructor
     *
     * @param config PostgresSinkConnectorConfig the config
     * @param isConnectionAlive AtomicBoolean the isConnectionAlive
     */
    public ConnectionInfo(PostgresSinkConnectorConfig config, AtomicBoolean isConnectionAlive) {
        this.username = config.databaseUsername;
        this.password = config.databasePassword;
        this.ip = config.databaseIp;
        this.port = config.databasePort;
        this.database = config.databaseName;
        this.waitTimeoutSecond = config.getWaitTimeoutSecond();
        this.reconnectInterval = waitTimeoutSecond > DEFAULT_RECONNECT_INTERVAL ? DEFAULT_RECONNECT_INTERVAL : 1;
        this.isConnectionAlive = isConnectionAlive;
    }

    /**
     * Gets port
     *
     * @return int the port
     */
    public int getPort() {
        return port;
    }

    /**
     * Sets the port
     *
     * @param port int the port
     */
    public void setPort(int port) {
        this.port = port;
    }

    /**
     * Check connection whether valid
     *
     * @param connection the connection
     * @return boolean is or not valid
     */
    public boolean checkConnectionStatus(Connection connection) {
        try {
            if (connection.isValid(1)) {
                return true;
            } else {
                LOGGER.error("There is a connection problem with the openGauss,"
                        + " check the database status or connection");
                return false;
            }
        } catch (SQLException exception) {
            LOGGER.error("the cause of the exception is {}", exception.getMessage());
        }
        return false;
    }

    /**
     * Create openGauss connection
     *
     * @return Connection the connection
     */
    public Connection createOpenGaussConnection() {
        String url = String.format("jdbc:opengauss://%s:%s/%s?loggerLevel=OFF", ip, port, database);
        String driver = OPENGAUSS_JDBC_DRIVER;
        Connection connection = null;
        PreparedStatement ps = null;
        long totalReconnectCount = waitTimeoutSecond % reconnectInterval == 0 ? waitTimeoutSecond / reconnectInterval
                : waitTimeoutSecond / reconnectInterval + 1;
        long reconnectCount = 0L;
        while (true) {
            try {
                Class.forName(driver);
                connection = DriverManager.getConnection(url, username, password);
                isConnectionAlive.set(true);
                ps = connection.prepareStatement("set session_timeout = 0");
                ps.execute();
                return connection;
            } catch (ClassNotFoundException | SQLException exp) {
                isConnectionAlive.set(false);
                reconnectCount++;
                if (waitTimeoutSecond > 0) {
                    if (reconnectInterval * reconnectCount >= waitTimeoutSecond) {
                        break;
                    }
                    LOGGER.warn("The target database occurred an exception: {}, {} th reconnect,"
                            + " will try up to {} times.", exp, reconnectCount, totalReconnectCount);
                } else {
                    LOGGER.warn("The target database occurred an exception: {}, {} th reconnect.", exp, reconnectCount);
                }
                MigrationProcessController.sleep(reconnectInterval * 1000L);
            } finally {
                if (ps != null) {
                    try {
                        ps.close();
                    } catch (SQLException e) {
                        LOGGER.error("Failed to close PreparedStatement.", e);
                    }
                }
            }
        }
        LOGGER.error("{} Create openGauss connection failed.", ErrorCode.DB_CONNECTION_EXCEPTION);
        return connection;
    }
}
