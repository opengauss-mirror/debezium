/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package io.debezium.connector.postgresql.sink.object;

import java.util.List;

/**
 * TableMetaData
 *
 * @author tianbin
 * @since 2024-11-25
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
