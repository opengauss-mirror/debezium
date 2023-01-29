package io.debezium.connector.opengauss.sink.object;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TableMetaDataTest {
    @Test
    public void test(){
        String schemaName = "test_wzy";
        String tableName = "test1";
        List<ColumnMetaData> columnList = new ArrayList<>();
        columnList.add(new ColumnMetaData("id", "int"));
        columnList.add(new ColumnMetaData("name", "char(20)"));
        columnList.add(new ColumnMetaData("role", "char(20)"));
        TableMetaData tableMetaData = new TableMetaData(schemaName, tableName, columnList);
        Assert.assertNotNull(tableMetaData);
    }
}
