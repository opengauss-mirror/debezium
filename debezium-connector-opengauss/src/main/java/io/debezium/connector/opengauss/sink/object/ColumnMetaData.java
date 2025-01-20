/**
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.opengauss.sink.object;

/**
 * Description: ColumnMetaData
 * @author wangzhengyuan
 * @date 2022/11/04
 */

public class ColumnMetaData {
    private String columnName;
    private String columnType;
    private boolean isPrimaryKeyColumn = false;
    private Integer scale;
    private int length = 0;

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
     * @param scale Integer the scale
     */
    public ColumnMetaData(String columnName, String columnType, Integer scale) {
        this.columnName = columnName;
        this.columnType = columnType;
        this.scale = scale;
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
     * Sets boolean about primary key column
     *
     * @param primaryColumn boolean the isPrimaryColumn
     */
    public void setPrimaryKeyColumn(boolean primaryColumn) {
        isPrimaryKeyColumn = primaryColumn;
    }

    public Integer getScale() {
        return scale;
    }

    public void setScale(Integer scale) {
        this.scale = scale;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }
}
