/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.sink.object;

import io.debezium.connector.mysql.sink.util.SqlTools;

/**
 * Description: ColumnMetaData class
 * @author douxin
 * @date 2022/10/31
 **/
public class ColumnMetaData {
    private String columnName;
    private String columnType;
    private Integer scale;
    private boolean isPrimaryColumn;

    /**
     * Constructor
     *
     * @param String the column name
     * @param String the column type
     * @param Integer the column scale
     */
    public ColumnMetaData(String columnName, String columnType, Integer scale) {
        this.columnName = columnName;
        this.columnType = columnType;
        this.scale = scale;
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
     * @param String the column name
     */
    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    /**
     * Is primary key column
     *
     * @return boolean the isPrimaryColumn
     */
    public boolean isPrimaryColumn() {
        return isPrimaryColumn;
    }

    /**
     * Sets boolean about primary key column
     *
     * @param primaryColumn boolean the isPrimaryColumn
     */
    public void setPrimaryColumn(boolean primaryColumn) {
        isPrimaryColumn = primaryColumn;
    }

    /**
     * Gets column numeric_scale
     *
     * @return String the column numeric_scale
     */
    public Integer getScale() {
        return scale;
    }

    /**
     * Gets column type
     *
     * @return String the column type
     */
    public String getColumnType() {
        return columnType;
    }

    /**
     * Sets column type
     *
     * @param String the column type
     */
    public void setColumnType(String columnType) {
        this.columnType = columnType;
    }

    /**
     * Get wrapped column name
     *
     */
    public String getWrappedColumnName() {
        return SqlTools.addingQuote(columnName);
    }
}
