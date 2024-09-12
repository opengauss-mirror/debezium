/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.sink.object;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicBoolean;

import io.debezium.connector.mysql.sink.task.MySqlSinkConnectorConfig;
import io.debezium.util.MigrationProcessController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Description: ConnectionInfo class
 * @author douxin
 * @date 2022/10/31
 **/
public class ConnectionInfo {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionInfo.class);

    /**
     * The openGauss JDBC driver class
     */
    private static final String OPENGAUSS_JDBC_DRIVER = "org.opengauss.Driver";
    private static final int DEFAULT_RECONNECT_INTERVAL = 5;

    private String host;
    private int port;
    private String database;
    private String username;
    private String password;
    private String url;
    private long waitTimeoutSecond;
    private int reconnectInterval;
    private AtomicBoolean isConnectionAlive;

    /**
     * Constructor
     */
    public ConnectionInfo() {
    }

    /**
     * Constructor
     *
     * @param String the host
     * @param int the port
     * @param String the database
     * @param String the username
     * @param String the password
     */
    public ConnectionInfo(String host, int port, String database, String username, String password) {
        this.host = host;
        this.port = port;
        this.database = database;
        this.username = username;
        this.password = password;
    }

    /**
     * Constructor
     *
     * @param String the url
     * @param String the username
     * @param String the password
     */
    public ConnectionInfo(String url, String username, String password) {
        this.url = url;
        this.username = username;
        this.password = password;
    }

    /**
     * Constructor
     *
     * @param config MySqlSinkConnectorConfig the config
     * @param isConnectionAlive AtomicBoolean the isConnectionAlive
     */
    public ConnectionInfo(MySqlSinkConnectorConfig config, AtomicBoolean isConnectionAlive) {
        this.username = config.openGaussUsername;
        this.password = config.openGaussPassword;
        this.waitTimeoutSecond = config.getWaitTimeoutSecond();
        this.reconnectInterval = waitTimeoutSecond > DEFAULT_RECONNECT_INTERVAL ? DEFAULT_RECONNECT_INTERVAL : 1;
        this.isConnectionAlive = isConnectionAlive;
        constructUrl(config);
    }

    private void constructUrl(MySqlSinkConnectorConfig config) {
        if (MigrationProcessController.isNullOrBlank(config.getDbStandbyHostnames())
                || MigrationProcessController.isNullOrBlank(config.getDbStandbyPorts())) {
            this.url = config.openGaussUrl;
        } else {
            String originUrl = config.openGaussUrl;
            String primary = originUrl.substring(originUrl.indexOf("//") + 2, originUrl.lastIndexOf("/"));
            StringBuilder sb = new StringBuilder(primary);
            String[] hostnames = config.getDbStandbyHostnames().split(",");
            String[] ports = config.getDbStandbyPorts().split(",");
            int end = Math.min(hostnames.length, ports.length);
            for (int i = 0; i < end; i++) {
                sb.append(",").append(hostnames[i]).append(":").append(ports[i]);
            }
            this.url = originUrl.replaceAll(primary, sb.toString()) + "&targetServerType=master";
            LOGGER.info("openGauss is in the form of primary and standby deployment, jdbc url is {}", url);
        }
    }

    /**
     * Sets host
     *
     * @param String the host
     */
    public void setHost(String host) {
        this.host = host;
    }

    /**
     * Gets host
     *
     * @return String the host
     */
    public String getHost() {
        return host;
    }

    /**
     * Sets port
     *
     * @param int the port
     */
    public void setPort(int port) {
        this.port = port;
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
     * Sets database
     *
     * @param String the database
     */
    public void setDatabase(String database) {
        this.database = database;
    }

    /**
     * Gets database
     *
     * @return String the database
     */
    public String getDatabase() {
        return database;
    }

    /**
     * Sets username
     *
     * @param String the username
     */
    public void setUsername(String username) {
        this.username = username;
    }

    /**
     * Gets username
     *
     * @return String the username
     */
    public String getUsername() {
        return username;
    }

    /**
     * Sets password
     *
     * @param String the password
     */
    public void setPassword(String password) {
        this.password = password;
    }

    /**
     * Gets password
     *
     * @return String the password
     */
    public String getPassword() {
        return password;
    }

    /**
     * Create openGauss connection
     *
     * @return Connection the connection
     */
    public Connection createOpenGaussConnection() {
        String driver = OPENGAUSS_JDBC_DRIVER;
        Connection connection;
        long reconnectCount = 0L;
        long totalReconnectCount = waitTimeoutSecond % reconnectInterval == 0 ? waitTimeoutSecond / reconnectInterval
                : waitTimeoutSecond / reconnectInterval + 1;
        while (true) {
            try {
                Class.forName(driver);
                connection = DriverManager.getConnection(url, username, password);
                Statement stmt = connection.createStatement();
                stmt.execute("set session_timeout = 0");
                stmt.execute("set dolphin.b_compatibility_mode to on");
                stmt.close();
                LOGGER.info("Connected to sink database successfully.");
                isConnectionAlive.set(true);
                return connection;
            } catch (ClassNotFoundException | SQLException exp) {
                isConnectionAlive.set(false);
                reconnectCount++;
                if (waitTimeoutSecond > 0) {
                    if (reconnectInterval * reconnectCount >= waitTimeoutSecond) {
                        break;
                    }
                    LOGGER.warn("The target database occurred an exception, {} th reconnect, will try up to {} times.",
                            reconnectCount, totalReconnectCount);
                } else {
                    LOGGER.warn("The target database occurred an exception, {} th reconnect.", reconnectCount);
                }
                MigrationProcessController.sleep(reconnectInterval * 1000L);
            }
        }
        throw new RuntimeException("Could not reconnect due to sink database shutdown service.");
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
            }
            else {
                LOGGER.warn("There is a connection problem with the openGauss,"
                        + " check the database status or connection");
                return false;
            }
        }
        catch (SQLException exception) {
            LOGGER.error("the cause of the exception is {}", exception.getMessage());
        }
        return false;
    }
}
