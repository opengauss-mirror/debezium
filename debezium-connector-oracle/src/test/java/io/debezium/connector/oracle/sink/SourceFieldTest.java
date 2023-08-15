/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.sink;

import org.junit.Assert;
import org.junit.Test;

import io.debezium.connector.oracle.sink.object.SourceField;

/**
 * Description: SourceFieldTest class
 *
 * @author gbase
 * @date 2023/08/02
 **/
public class SourceFieldTest {
    @Test
    public void test() {
        Assert.assertEquals("source", SourceField.SOURCE);
        Assert.assertEquals("db", SourceField.DATABASE);
        Assert.assertEquals("schema", SourceField.SCHEMA);
        Assert.assertEquals("table", SourceField.TABLE);
        Assert.assertEquals("txId", SourceField.TXID);
        Assert.assertEquals("scn", SourceField.SCN);
        Assert.assertEquals("commit_scn", SourceField.COMMIT_SCN);
        Assert.assertEquals("sequence", SourceField.SEQUENCE);
        Assert.assertEquals("snapshot", SourceField.SNAPSHOT);
    }
}
