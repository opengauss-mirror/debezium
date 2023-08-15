/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.sink;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import io.debezium.connector.oracle.sink.object.ConnectionInfo;

/**
 * Description: ConnectionInfoTest class
 *
 * @author gbase
 * @date 2023/08/02
 **/
public class ConnectionInfoTest {
    private static String host;
    private static int port;
    private static String database;
    private static String username;
    private static String password;

    @BeforeClass
    public static void setUp() {
        host = "127.0.0.1";
        port = 5432;
        database = "postgres";
        username = "opengauss";
        password = "Test@123";
    }

    @Test
    public void test1() {
        ConnectionInfo connectionInfo = new ConnectionInfo();
        connectionInfo.setHost(host);
        connectionInfo.setPort(port);
        connectionInfo.setDatabase(database);
        connectionInfo.setUsername(username);
        connectionInfo.setPassword(password);
        Assert.assertEquals(host, connectionInfo.getHost());
        Assert.assertEquals(port, connectionInfo.getPort());
        Assert.assertEquals(database, connectionInfo.getDatabase());
        Assert.assertEquals(username, connectionInfo.getUsername());
        Assert.assertEquals(password, connectionInfo.getPassword());
    }

    @Test
    public void test2() {
        ConnectionInfo connectionInfo = new ConnectionInfo(host, port, database, username, password);
        Assert.assertEquals(host, connectionInfo.getHost());
        Assert.assertEquals(port, connectionInfo.getPort());
        Assert.assertEquals(database, connectionInfo.getDatabase());
        Assert.assertEquals(username, connectionInfo.getUsername());
        Assert.assertEquals(password, connectionInfo.getPassword());
    }
}
