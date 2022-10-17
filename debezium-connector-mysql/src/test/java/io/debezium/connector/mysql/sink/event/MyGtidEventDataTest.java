/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.sink.event;

import org.junit.Assert;
import org.junit.Test;

/**
 * Description: MyGtidEventDataTest class
 *
 * @author douxin
 * @date 2022/11/25
 **/
public class MyGtidEventDataTest {
    @Test
    public void test() {
        MyGtidEventData myGtidEventData = new MyGtidEventData();
        myGtidEventData.setLastCommitted(1);
        myGtidEventData.setSequenceNumber(2);
        Assert.assertEquals(1, myGtidEventData.getLastCommitted());
        Assert.assertEquals(2, myGtidEventData.getSequenceNumber());
        Assert.assertTrue(myGtidEventData.toString().contains("lastCommitted"));
    }
}
