package io.debezium.connector.opengauss.sink.object;

import org.junit.Assert;
import org.junit.Test;

public class ColumnMetaDataTest {
    @Test
    public void test() {
        ColumnMetaData columnMetaData = new ColumnMetaData("id", "int");
        Assert.assertEquals("id", columnMetaData.getColumnName());
        Assert.assertEquals("int", columnMetaData.getColumnType());
        columnMetaData.setColumnName("name");
        columnMetaData.setColumnType("varchar");
    }
}