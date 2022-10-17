/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.sink.object;

import org.junit.Assert;
import org.junit.Test;

/**
 * Description: SourceFieldTest class
 *
 * @author douxin
 * @date 2022/11/25
 **/
public class SourceFieldTest {
    @Test
    public void test() {
        Assert.assertEquals("source", SourceField.SOURCE);
        Assert.assertEquals("db", SourceField.DATABASE);
        Assert.assertEquals("table", SourceField.TABLE);
        Assert.assertEquals("gtid", SourceField.GTID);
        Assert.assertEquals("last_committed", SourceField.LAST_COMMITTED);
        Assert.assertEquals("sequence_number", SourceField.SEQUENCE_NUMBER);
        Assert.assertEquals("file", SourceField.FILE);
        Assert.assertEquals("pos", SourceField.POSITION);
    }
}
