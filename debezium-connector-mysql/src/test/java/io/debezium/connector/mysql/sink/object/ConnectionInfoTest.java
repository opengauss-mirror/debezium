/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.sink.object;

import org.junit.Assert;
import org.junit.Test;

/**
 * Description: ConnectionInfoTest class
 *
 * @author douxin
 * @date 2022/11/25
 **/
public class ConnectionInfoTest {
    @Test
    public void test() {
        ConnectionInfo connectionInfo = new ConnectionInfo();
        connectionInfo.setHost("127.0.0.1");
        connectionInfo.setPort(5432);
        connectionInfo.setDatabase("postgres");
        connectionInfo.setUsername("opengauss");
        connectionInfo.setPassword("Test@123");
        Assert.assertEquals("127.0.0.1", connectionInfo.getHost());
        Assert.assertEquals(5432, connectionInfo.getPort());
        Assert.assertEquals("postgres", connectionInfo.getDatabase());
        Assert.assertEquals("opengauss", connectionInfo.getUsername());
        Assert.assertEquals("Test@123", connectionInfo.getPassword());
        ConnectionInfo connectionInfo1 = new ConnectionInfo(
                "jdbc:opengauss://127.0.0.1:5432/postgres?loggerLevel=OFF",
                "opengauss", "Test@123");
        Assert.assertNotNull(connectionInfo1);
        ConnectionInfo connectionInfo2 = new ConnectionInfo("127.0.0.1", 5432,
                "postgres", "opengauss", "Test@123");
        Assert.assertNotNull(connectionInfo2);
    }
}
