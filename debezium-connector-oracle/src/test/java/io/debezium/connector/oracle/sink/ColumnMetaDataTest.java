/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.sink;

import org.junit.Assert;
import org.junit.Test;

import io.debezium.connector.oracle.sink.object.ColumnMetaData;

/**
 * Description: ColumnMetaDataTest class
 *
 * @author gbase
 * @date 2023/08/02
 **/
public class ColumnMetaDataTest {
    @Test
    public void test() {
        String columnName = "id";
        String columnType = "int";
        Integer scale = 0;
        String intervalType = "YEAR TO MONTH";
        ColumnMetaData columnMetaData = new ColumnMetaData(columnName, columnType, scale, intervalType);
        Assert.assertEquals(columnName, columnMetaData.getColumnName());
        Assert.assertEquals(columnType, columnMetaData.getColumnType());
        Assert.assertEquals(scale, columnMetaData.getScale());
        Assert.assertEquals(intervalType, columnMetaData.getIntervalType());

        columnMetaData.setPrimaryColumn(true);
        Assert.assertTrue(columnMetaData.isPrimaryColumn());
    }
}
