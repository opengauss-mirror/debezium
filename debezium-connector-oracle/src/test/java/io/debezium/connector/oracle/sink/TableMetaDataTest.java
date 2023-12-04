/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.sink;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import io.debezium.connector.oracle.sink.object.ColumnMetaData;
import io.debezium.connector.oracle.sink.object.TableMetaData;

/**
 * Description: TableMetaDataTest class
 *
 * @author gbase
 * @date 2023/08/02
 **/
public class TableMetaDataTest {
    @Test
    public void test() {
        String schemaName = "public";
        String tableName = "table1";
        List<ColumnMetaData> columnMetaData = new ArrayList<>();
        columnMetaData.add(new ColumnMetaData("id", "int", 0, ""));
        columnMetaData.add(new ColumnMetaData("name", "varchar", 0, ""));

        TableMetaData tableMetaData = new TableMetaData(schemaName, tableName, columnMetaData);
        Assert.assertNotNull(tableMetaData);
        tableMetaData.setSchemaName("schema1");
        tableMetaData.setTableName("table2");
        columnMetaData.add(new ColumnMetaData("flag", "boolean", 0, ""));
        tableMetaData.setColumnList(columnMetaData);

        Assert.assertEquals("schema1", tableMetaData.getSchemaName());
        Assert.assertEquals("table2", tableMetaData.getTableName());
        Assert.assertEquals(3, tableMetaData.getColumnList().size());
    }
}
