/**
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.opengauss.sink.object;

import java.util.List;

/**
 * Description: TableMetaData
 * @author wangzhengyuan
 * @date 2022/11/07
 */
public class TableMetaData {
    private String schemaName;
    private String tableName;
    private List<ColumnMetaData> columnList;

    /**
     * Constructor
     *
     * @param schemaName String the schema name
     * @param tableName String the table name
     * @param columnList List<ColumnMetaData> the columnList
     */
    public TableMetaData(String schemaName, String tableName, List<ColumnMetaData> columnList) {
        this.tableName = tableName;
        this.columnList = columnList;
        this.schemaName = schemaName;
    }

    /**
     * Gets table name
     *
     * @return String the table name
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Sets table name
     *
     * @param tableName String the table name
     */
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    /**
     * Gets schema name
     *
     * @return String the schema name
     */
    public String getSchemaName() {
        return schemaName;
    }

    /**
     * Gets column list
     *
     * @return List<ColumnMetaData> the column list
     */
    public List<ColumnMetaData> getColumnList() {
        return columnList;
    }

}
