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
    public static final String MYSQL_JDBC_DRIVER = "com.mysql.jdbc.Driver";

    private String host;
    private Integer port;
    private String database;
    private String username;
    private String password;
    private String url;
    private String dbUrl;

    /**
     * Constructor
     */
    public ConnectionInfo() {
    }

    /**
     * Constructor
     *
     * @param url String the url
     * @param username String the username
     * @param password String the password
     * @param database String the database
     * @param port int the port
     */
    public ConnectionInfo(String url, String username, String password, String database, Integer port) {
        this.url = url;
        this.username = username;
        this.password = password;
        this.database = database;
        this.port = port;
    }

    /**
     * Sets host
     *
     * @param host String the host
     */
    public void setHost(String host) {
        this.host = host;
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
     * Gets database
     *
     * @return String the database
     */
    public String getDatabase() {
        return database;
    }

    /**
     * Sets database
     *
     * @param database String the database
     */
    public void setDatabase(String database) {
        this.database = database;
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
     * Sets password
     *
     * @param password String the password
     */
    public void setPassword(String password) {
        this.password = password;
    }

    /**
     * Gets url
     *
     * @return String the url
     */
    public String getUrl() {
        return url;
    }

    /**
     * Sets url
     *
     * @param url String the url
     */
    public void setUrl(String url) {
        this.url = url;
    }

    /**
     * Sets username
     *
     * @param username String the username
     */
    public void setUsername(String username) {
        this.username = username;
    }

    /**
     * Sets database url
     *
     * @param dbUrl String the database url
     */
    public void setDbUrl(String dbUrl) {
        this.dbUrl = dbUrl;
    }

    /**
     * Gets host
     *
     * @return String the host
     */
    public String getHost() {
        return host;
    }

    public String getUsername() {
        return username;
    }

    /**
     * Gets database url
     *
     * @return String the database url
     */
    public String getDbUrl() {
        return dbUrl;
    }

    /**
     * Create mysql connection
     *
     * @return Connection the connection
     */
    public Connection createMysqlConnection(){
        dbUrl = "jdbc:mysql://" + url + ":" + port + "/" + database + "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";
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
