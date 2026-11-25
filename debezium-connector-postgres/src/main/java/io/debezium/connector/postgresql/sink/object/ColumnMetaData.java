/*
 * Copyright 2026 Huawei Technologies Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.debezium.connector.postgresql.sink.object;

import io.debezium.connector.postgresql.sink.utils.SqlTools;

/**
 * ColumnMetaData
 *
 * @author tianbin
 * @since 2024-11-25
 */

public class ColumnMetaData {
    private String columnName;
    private String columnType;
    private Integer scale;
    private int length = 0;
    private boolean isPrimaryKeyColumn = false;

    /**
     * Constructor
     *
     * @param columnName the column name
     * @param columnType the column type
     * @param scale the column scale
     */
    public ColumnMetaData(String columnName, String columnType, Integer scale) {
        this(columnName, columnType);
        this.scale = scale;
    }

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
        this(columnName, columnType);
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
     * @param columnName the column name
     */
    public void setColumnName(String columnName) {
        this.columnName = columnName;
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
     * Sets length
     *
     * @param length int the length
     */
    public void setLength(int length) {
        this.length = length;
    }

    /**
     * Gets length
     *
     * @return int the length
     */
    public int getLength() {
        return this.length;
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
     * @param columnType the column type
     */
    public void setColumnType(String columnType) {
        this.columnType = columnType;
    }

    /**
     * Get wrapped column name
     *
     * @return String the wrapped column name
     */
    public String getWrappedColumnName() {
        return SqlTools.addingQuote(columnName);
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
}
