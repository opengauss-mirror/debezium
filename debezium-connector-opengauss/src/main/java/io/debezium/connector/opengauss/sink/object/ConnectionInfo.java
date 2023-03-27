/**
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.opengauss.sink.object;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Description: ConnectionInfo class
 * @author wangzhengyuan
 * @date 2022/11/04
 */
public class ConnectionInfo {
    /**
     * The mysql JDBC driver class
     */
    public static final String MYSQL_JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";

    private Integer port;
    private String username;
    private String password;
    private String url;
    private String dbUrl;

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
        dbUrl = "jdbc:mysql://" + url + ":" + port + "/mysql?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC&autoReconnect=true";
        String driver = MYSQL_JDBC_DRIVER;
        Connection connection = null;
        try{
            Class.forName(driver);
            connection = DriverManager.getConnection(dbUrl, username,password);
        } catch (ClassNotFoundException | SQLException exp){
            exp.printStackTrace();
        }
        return connection;
    }
}