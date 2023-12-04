/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.sink;

import org.junit.Assert;
import org.junit.Test;

import io.debezium.connector.oracle.sink.object.TransactionRecordField;

/**
 * Description: TransactionRecordFieldTest class
 *
 * @author gbase
 * @date 2023/08/02
 **/
public class TransactionRecordFieldTest {
    @Test
    public void test() {
        Assert.assertEquals("id", TransactionRecordField.ID);
        Assert.assertEquals("status", TransactionRecordField.STATUS);
        Assert.assertEquals("event_count", TransactionRecordField.EVENT_COUNT);
        Assert.assertEquals("BEGIN", TransactionRecordField.BEGIN);
        Assert.assertEquals("END", TransactionRecordField.END);
    }
}
