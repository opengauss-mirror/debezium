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
import java.sql.SQLException;

/**
 * Description: ConnectionInfo class
 * @author wangzhengyuan
 * @date 2022/11/04
 */
public class ConnectionInfo {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionInfo.class);

    /**
     * The mysql JDBC driver class
     */
    public static final String MYSQL_JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";

    private Integer port;
    private final String username;
    private final String password;
    private final String url;

    /**
     * Constructor
     *
     * @param url String the url
     * @param username String the username
     * @param password String the password
     * @param port int the port
     */
    public ConnectionInfo(String url, String username, String password, Integer port) {
        this.url = url;
        this.username = username;
        this.password = password;
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
}