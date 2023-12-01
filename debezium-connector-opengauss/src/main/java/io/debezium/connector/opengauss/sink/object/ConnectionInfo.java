/**
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.opengauss.sink.object;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Description: ConnectionInfo class
 * @author wangzhengyuan
 * @date 2022/11/04
 */
public class ConnectionInfo {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionInfo.class);

    /**
     * The oracle JDBC driver class
     */
    private static final String ORACLE_JDBC_DRIVER = "oracle.jdbc.driver.OracleDriver";

    /**
     * The openGauss JDBC driver class
     */
    private static final String OPENGAUSS_JDBC_DRIVER = "org.postgresql.Driver";

    /**
     * The mysql JDBC driver class
     */
    public static final String MYSQL_JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";

    private Integer port;
    private final String username;
    private final String password;
    private final String url;
    private String databaseType = "mysql";
    private String database;

    /**
     * Constructor
     *
     * @param url String the url
     * @param username String the username
     * @param password String the password
     * @param port int the port
     */
    public ConnectionInfo(String url, String username, String password, Integer port, String databaseType) {
        this.url = url;
        this.username = username;
        this.password = password;
        this.port = port;
        this.databaseType = databaseType;
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
    public Connection createMysqlConnection(){
        String dbUrl = "jdbc:mysql://" + url + ":" + port + "/mysql?useSSL=false&allowPublicKeyRetrieval=true&"
                + "rewriteBatchedStatements=true&allowLoadLocalInfile=true&serverTimezone=UTC";
        Connection connection = null;
        try {
            Class.forName(MYSQL_JDBC_DRIVER);
            connection = DriverManager.getConnection(dbUrl, username, password);
        } catch (ClassNotFoundException | SQLException exp) {
            exp.printStackTrace();
        }
        return connection;
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
                LOGGER.error("There is a connection problem with the mysql,"
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
        String driver = OPENGAUSS_JDBC_DRIVER;
        Connection connection = null;
        try {
            Class.forName(driver);
            connection = DriverManager.getConnection(url, username, password);
            PreparedStatement ps = connection.prepareStatement("set session_timeout = 0");
            ps.execute();
        }
        catch (ClassNotFoundException | SQLException exp) {
            LOGGER.error("Create openGauss connection failed.", exp);
        }
        return connection;
    }

    /**
     * Create oracle connection
     *
     * @return Connection the connection
     */
    public Connection createOracleConnection() {
        String dbUrl = "jdbc:oracle:thin:@//" + url + ":" + port + "/" + database;
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
     * Set database
     *
     * @param database String the sid
     */
    public void setDatabase(String database) {
        this.database = database;
    }
}