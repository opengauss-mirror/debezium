/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.sink.object;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

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

    /**
     * The mysql JDBC driver class
     */
    private static final String MYSQL_JDBC_SRIVER = "com.mysql.cj.jdbc.Driver";

    private String host;
    private int port;
    private String database;
    private String username;
    private String password;
    private String url;

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
     * Create mysql connection
     *
     * @return Connection the Connection
     */
    public Connection createMysqlConnection() {
        String driver = MYSQL_JDBC_SRIVER;
        Connection connection = null;
        try {
            Class.forName(driver);
            connection = DriverManager.getConnection(url, username, password);
        }
        catch (ClassNotFoundException | SQLException exp) {
            LOGGER.error("Create mysql connection failed.", exp);
        }
        return connection;
    }
}
