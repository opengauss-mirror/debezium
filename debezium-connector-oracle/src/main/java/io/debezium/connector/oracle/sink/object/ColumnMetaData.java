/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.sink.object;

import io.debezium.connector.oracle.sink.util.SqlTools;

/**
 * Description: ColumnMetaData class
 * @author gbase
 * @date 2023/07/28
 **/
public class ColumnMetaData {
    private final String columnName;
    private final String columnType;
    private final Integer scale;
    private final String intervalType;
    private boolean isPrimaryColumn;

    /**
     * Constructor
     *
     * @param columnName the column name
     * @param columnType the column type
     * @param scale the column scale
     * @param intervalType the column interval type
     */
    public ColumnMetaData(String columnName, String columnType, Integer scale, String intervalType) {
        this.columnName = columnName;
        this.columnType = columnType;
        this.scale = scale;
        this.intervalType = intervalType;
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
     * Gets column interval type
     *
     * @return String the column interval type
     */
    public String getIntervalType() {
        return intervalType;
    }

    /**
     * Get wrapped column name
     *
     * @return String the wrapped column name
     */
    public String getWrappedColumnName() {
        return SqlTools.addingQuote(columnName);
    }
}
