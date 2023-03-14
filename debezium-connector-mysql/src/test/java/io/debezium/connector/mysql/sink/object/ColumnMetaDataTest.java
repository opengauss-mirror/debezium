/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.sink.object;

import org.junit.Assert;
import org.junit.Test;

/**
 * Description: ColumnMetaDataTest class
 *
 * @author douxin
 * @date 2022/11/25
 **/
public class ColumnMetaDataTest {
    @Test
    public void test() {
        ColumnMetaData columnMetaData = new ColumnMetaData("id", "int", 0);
        Assert.assertEquals("id", columnMetaData.getColumnName());
        Assert.assertEquals("int", columnMetaData.getColumnType());
        columnMetaData.setColumnName("name");
        columnMetaData.setColumnType("varchar");
    }
}
