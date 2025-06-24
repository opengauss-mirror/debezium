/*
 * Copyright (c) 2025-2025 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *           http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
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

    /**
     * Constructor
     *
     * @param rs resultSet
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
}
