/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
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
