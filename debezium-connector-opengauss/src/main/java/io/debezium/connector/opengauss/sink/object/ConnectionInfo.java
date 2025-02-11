/**
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.opengauss.sink.object;

import io.debezium.connector.opengauss.sink.task.OpengaussSinkConnectorConfig;
import io.debezium.enums.ErrorCode;
import io.debezium.util.MigrationProcessController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Description: ConnectionInfo class
 * @author wangzhengyuan
 * @date 2022/11/04
 */
public class ConnectionInfo {
    /**
     * The postgres JDBC driver class
     */
    public static final String POSTGRES_JDBC_DRIVER = "org.postgresql.Driver";

    /**
     * The mysql JDBC driver class
     */
    public static final String MYSQL_JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";

    /**
     * The oracle JDBC driver class
     */
    private static final String ORACLE_JDBC_DRIVER = "oracle.jdbc.driver.OracleDriver";

    /**
     * The openGauss JDBC driver class
     */
    private static final String OPENGAUSS_JDBC_DRIVER = "org.opengauss.Driver";

    private static final int DEFAULT_RECONNECT_INTERVAL = 5;

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionInfo.class);

    private Integer port;
    private final String username;
    private final String password;
    private final String ip;
    private final AtomicBoolean isConnectionAlive;
    private String databaseType = "mysql";
    private String database;
    private long waitTimeoutSecond;
    private int reconnectInterval;

    /**
     * Constructor
     *
     * @param config OpengaussSinkConnectorConfig the config
     * @param isConnectionAlive AtomicBoolean the isConnectionAlive
     */
    public ConnectionInfo(OpengaussSinkConnectorConfig config, AtomicBoolean isConnectionAlive) {
        this.username = config.databaseUsername;
        this.password = config.databasePassword;
        this.ip = config.databaseIp;
        this.port = config.databasePort;
        this.database = config.databaseName;
        this.databaseType = config.databaseType;
        this.waitTimeoutSecond = config.getWaitTimeoutSecond();
        this.reconnectInterval = waitTimeoutSecond > DEFAULT_RECONNECT_INTERVAL ? DEFAULT_RECONNECT_INTERVAL : 1;
        this.isConnectionAlive = isConnectionAlive;
    }

    public String getDatabaseType() {
        return databaseType;
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
     * Create mysql connection
     *
     * @return Connection the connection
     */
    public Connection createMysqlConnection() {
        String dbUrl = "jdbc:mysql://" + ip + ":" + port + "/mysql?useSSL=false&allowPublicKeyRetrieval=true&"
                + "rewriteBatchedStatements=true&allowLoadLocalInfile=true&serverTimezone=UTC";
        Connection connection;
        long reconnectCount = 0L;
        long totalReconnectCount = waitTimeoutSecond % reconnectInterval == 0 ? waitTimeoutSecond / reconnectInterval
                : waitTimeoutSecond / reconnectInterval + 1;
        while (true) {
            try {
                Class.forName(MYSQL_JDBC_DRIVER);
                connection = DriverManager.getConnection(dbUrl, username, password);
                isConnectionAlive.set(true);
                LOGGER.info("Connected to sink database successfully.");
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
            } else {
                LOGGER.error("{}There is a connection problem with the mysql,"
                        + " check the database status or connection", ErrorCode.DB_CONNECTION_EXCEPTION);
                return false;
            }
        } catch (SQLException exception) {
            LOGGER.error("{}the cause of the exception is {}", ErrorCode.SQL_EXCEPTION, exception.getMessage());
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
        try {
            Class.forName(driver);
            connection = DriverManager.getConnection(url, username, password);
            PreparedStatement ps = connection.prepareStatement("set session_timeout = 0");
            ps.execute();
        }
        catch (ClassNotFoundException | SQLException exp) {
            LOGGER.error("{}Create openGauss connection failed.", ErrorCode.DB_CONNECTION_EXCEPTION, exp);
        }
        return connection;
    }

    /**
     * Create oracle connection
     *
     * @return Connection the connection
     */
    public Connection createOracleConnection() {
        String dbUrl = "jdbc:oracle:thin:@//" + ip + ":" + port + "/" + database;
        String driver = ORACLE_JDBC_DRIVER;
        Connection connection = null;
        try {
            Class.forName(driver);
            connection = DriverManager.getConnection(dbUrl, username, password);
        } catch (ClassNotFoundException | SQLException exp) {
            exp.printStackTrace();
        }
        return connection;
    }

    /**
     * Create postgres connection
     *
     * @return Connection the connection
     */
    public Connection createPostgresConnection() {
        String url = String.format("jdbc:postgresql://%s:%s/%s?loggerLevel=OFF", ip, port, database);
        String driver = POSTGRES_JDBC_DRIVER;
        Connection connection = null;
        long reconnectCount = 0L;
        long totalReconnectCount = waitTimeoutSecond % reconnectInterval == 0 ? waitTimeoutSecond / reconnectInterval
                : waitTimeoutSecond / reconnectInterval + 1;
        PreparedStatement ps = null;
        while (true) {
            try {
                Class.forName(driver);
                connection = DriverManager.getConnection(url, username, password);
                isConnectionAlive.set(true);
                ps = connection.prepareStatement("set statement_timeout = 0");
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
                        LOGGER.error("{} Close preparedStatement failed, ", ErrorCode.DB_CONNECTION_EXCEPTION, e);
                    }
                }
            }
        }
        LOGGER.error("{} Create openGauss connection failed.", ErrorCode.DB_CONNECTION_EXCEPTION);
        return connection;
    }
}