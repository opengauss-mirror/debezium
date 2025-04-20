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
 * TableForeignKey
 *
 * @since 2025-04-18
 */
@Data
@NoArgsConstructor
public class TableForeignKey {
    private String schemaName;
    private String tableName;
    private String fkName;
    private String parentTable;
    private String referencedTable;
    private String parentColumn;
    private String referencedColumn;

    /**
     * Constructor
     *
     * @param rs result set
     * @throws SQLException SQLException
     */
    public TableForeignKey(ResultSet rs) throws SQLException {
        this.fkName = rs.getString("fk_name");
        this.parentTable = rs.getString("parent_table");
        this.referencedTable = rs.getString("referenced_table");
        this.parentColumn = rs.getString("parent_column");
        this.referencedColumn = rs.getString("referenced_column");
    }
}
