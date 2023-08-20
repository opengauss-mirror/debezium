/**
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.opengauss.sink.object;

import io.debezium.connector.opengauss.sink.utils.SqlTools;

/**
 * Description: ColumnMetaData
 * @author wangzhengyuan
 * @date 2022/11/04
 */

public class ColumnMetaData {
    private String columnName;
    private String columnType;
    private boolean isPrimaryKeyColumn = false;

    /**
     * Constructor
     *
     * @param columnName String the column name
     * @param columnType String the column type
     */
    public ColumnMetaData(String columnName, String columnType) {
        this.columnName = columnName;
        this.columnType = columnType;
    }

    /**
     * Constructor
     *
     * @param columnName String the column name
     * @param columnType String the column type
     * @param isPrimaryKeyColumn boolean the isPrimaryKeyColumn
     */
    public ColumnMetaData(String columnName, String columnType, boolean isPrimaryKeyColumn) {
        this.columnName = columnName;
        this.columnType = columnType;
        this.isPrimaryKeyColumn = isPrimaryKeyColumn;
    }

    /**
     * Gets column name
     *
     * @return String the column name
     */
    public String getColumnName() {
        return columnName;
    }

    /**
     * Sets column name
     *
     * @param columnName String the column name
     */
    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    /**
     * Gets column type
     *
     * @return String th column type
     */
    public String getColumnType() {
        return columnType;
    }

    /**
     * Sets column type
     *
     * @param columnType String the column type
     */
    public void setColumnType(String columnType) {
        this.columnType = columnType;
    }

    /**
     * Is primary key column
     *
     * @return boolean the isPrimaryKeyColumn
     */
    public boolean isPrimaryKeyColumn() {
        return isPrimaryKeyColumn;
    }

    /**
     * Get wrapped column name
     *
     */
    public String getWrappedColumnName() {
        return SqlTools.addingBackQuote(columnName);
    }
}
