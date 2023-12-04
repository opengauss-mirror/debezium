/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.sink.object;

import java.util.List;

import io.debezium.connector.oracle.sink.util.SqlTools;

/**
 * Description: TableMetaData class
 * @author gbase
 * @date 2023/07/28
 **/
public class TableMetaData {
    private String schemaName;
    private String tableName;
    private List<ColumnMetaData> columnList;

    /**
     * Constructor
     *
     * @param schemaName the schema name
     * @param tableName the table name
     * @param columnList the column list
     */
    public TableMetaData(String schemaName, String tableName, List<ColumnMetaData> columnList) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.columnList = columnList;
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
     * Sets schema name
     *
     * @param schemaName the schema name
     */
    public void setSchemaName(String schemaName) {
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
     * @param tableName the table name
     */
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    /**
     * Gets column list
     *
     * @return List<ColumnMetaData> the column list
     */
    public List<ColumnMetaData> getColumnList() {
        return columnList;
    }

    /**
     * Sets column list
     *
     * @param columnList the column list
     */
    public void setColumnList(List<ColumnMetaData> columnList) {
        this.columnList = columnList;
    }

    /**
     * Get table full name
     *
     * @return String the table full name
     */
    public String getTableFullName() {
        return SqlTools.addingQuote(schemaName) + "." + SqlTools.addingQuote(tableName);
    }
}
