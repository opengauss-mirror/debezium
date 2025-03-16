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

package org.full.migration.source;

import com.microsoft.sqlserver.jdbc.StringUtils;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import org.full.migration.jdbc.SqlServerConnection;
import org.full.migration.object.SourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SqlServerSource
 *
 * @since 2025-03-15
 */
@EqualsAndHashCode(callSuper = true)
@Data
@NoArgsConstructor
public class SqlServerSource extends SourceDatabase {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerSource.class);

    private static final String QUERY_TABLE_SQL = "SELECT " + "    o.name AS tableName, " + "    p.rows AS tableRows, "
        + "    CASE " + "        WHEN p.rows > 0 THEN SUM(a.total_pages * 8) / p.rows " + "        ELSE 0 "
        + "    END AS avgRowLength " + "FROM " + "    sys.objects o "
        + "    JOIN sys.partitions p ON o.object_id = p.object_id "
        + "    JOIN sys.allocation_units a ON p.partition_id = a.container_id " + "WHERE " + "    o.type = 'U' "
        + "    AND o.schema_id = SCHEMA_ID('%s')" + "    GROUP BY " + "    o.name, p.rows " + "    ORDER BY "
        + "    p.rows ASC;";

    private static final String LOCK_TABLE_SQL = "BEGIN TRANSACTION;\n" + "SELECT * FROM [%s].[%s] WITH (HOLDLOCK);";

    /**
     * Constructor
     *
     * @param sourceConfig sourceConfig
     */
    public SqlServerSource(SourceConfig sourceConfig) {
        super(sourceConfig);
        this.connection = new SqlServerConnection();
    }

    @Override
    protected String getQueryTableSql(String schema) {
        return String.format(QUERY_TABLE_SQL, schema);
    }

    @Override
    protected String getLockSql(String schema, String table) {
        return String.format(LOCK_TABLE_SQL, schema, table);
    }

    @Override
    protected String getQueryObjectSql(String objectType) {
        switch (objectType) {
            case "view":
                return "SELECT name, OBJECT_DEFINITION(OBJECT_ID) AS definition FROM sys.views WHERE is_ms_shipped = 0";
            case "function":
                return "SELECT name, OBJECT_DEFINITION(OBJECT_ID) AS definition FROM sys.objects WHERE type IN ('FN',"
                    + " 'IF', 'TF') AND is_ms_shipped = 0";
            case "trigger":
                return "SELECT name, OBJECT_DEFINITION(OBJECT_ID) AS definition FROM sys.triggers WHERE is_ms_shipped"
                    + " = 0";
            case "procedure":
                return "SELECT name, OBJECT_DEFINITION(OBJECT_ID) AS definition FROM sys.objects WHERE type = 'P' AND"
                    + " is_ms_shipped = 0";
            default:
                LOGGER.error("");
                System.exit(-1);
        }
        return StringUtils.EMPTY;
    }

    @Override
    protected String convertDefinition(String definition) {
        return definition;
    }
}
