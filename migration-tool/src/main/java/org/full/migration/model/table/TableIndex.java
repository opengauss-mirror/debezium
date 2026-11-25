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

package org.full.migration.model.table;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * TableIndex
 *
 * @since 2025-04-18
 */
@Data
@NoArgsConstructor
public class TableIndex {
    private String schemaName;
    private String tableName;
    private String columnName;
    private String includedColumns;
    private String indexName;
    private String indexType;
    private boolean isPrimaryKey;
    private boolean isUnique;
    private boolean hasFilter;
    private String filterDefinition;
    private String indexprs;
    private String indexRange;
    private boolean isConstraint;
    private String indexDDL;

    /**
     * Constructor
     *
     * @param rs ResultSet
     * @throws SQLException SQLException
     */
    public TableIndex(ResultSet rs) throws SQLException {
        this.indexName = rs.getString("index_name");
        this.tableName = rs.getString("table_name");
        this.indexType = rs.getString("type_desc");
        this.isUnique = rs.getBoolean("is_unique");
        this.isPrimaryKey = rs.getBoolean("is_primary_key");
        this.hasFilter = rs.getBoolean("has_filter");
        this.filterDefinition = rs.getString("filter_definition");
    }

    @Override
    public String toString() {
        return "TableIndex{" + tableName + ", " + indexName + ", " + indexType +'}';
    }
}
