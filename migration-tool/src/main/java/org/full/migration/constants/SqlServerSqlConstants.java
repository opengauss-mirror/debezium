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

package org.full.migration.constants;

/**
 * SqlServerSqlConstants
 *
 * @since 2025-04-18
 */
public final class SqlServerSqlConstants {
    /**
     * sql for querying all tables
     */
    public static final String QUERY_TABLE_SQL = "WITH TableStats AS (\n" + "    SELECT \n"
        + "        o.name AS tableName,\n" + "        o.object_id,\n" + "        SUM(p.rows) AS tableRows,\n"
        + "        (SELECT SUM(a2.total_pages * 8) \n" + "         FROM sys.partitions p2 \n"
        + "         JOIN sys.allocation_units a2 ON p2.partition_id = a2.container_id\n"
        + "         WHERE p2.object_id = o.object_id) AS totalPageSize\n" + "    FROM \n" + "        sys.objects o\n"
        + "        JOIN sys.partitions p ON o.object_id = p.object_id\n" + "    WHERE \n" + "        o.type = 'U'\n"
        + "        AND o.schema_id = SCHEMA_ID('%s')\n" + "    GROUP BY \n" + "        o.name, o.object_id\n" + "),\n"
        + "PartitionInfo AS (\n" + "    SELECT \n" + "        i.object_id,\n"
        + "        CAST(1 AS BIT) AS isPartitioned\n" + "    FROM \n" + "        sys.indexes i\n"
        + "        JOIN sys.partition_schemes ps ON i.data_space_id = ps.data_space_id\n" + "    WHERE \n"
        + "        i.index_id IN (0, 1)\n" + ")\n" + "SELECT \n" + "    ts.tableName,\n" + "    ts.tableRows,\n"
        + "    CASE \n" + "        WHEN ts.tableRows > 0 THEN ts.totalPageSize / ts.tableRows\n" + "        ELSE 0 \n"
        + "    END AS avgRowLength,\n" + "    CASE \n" + "        WHEN pi.object_id IS NOT NULL THEN 1\n"
        + "        ELSE 0 \n" + "    END AS isPartitioned\n" + "FROM \n" + "    TableStats ts\n"
        + "    LEFT JOIN PartitionInfo pi ON ts.object_id = pi.object_id\n" + "ORDER BY \n" + "    ts.tableRows ASC";

    /**
     * sql for querying generate define
     */
    public static final String QUERY_GENERATE_DEFINE_SQL = "SELECT \n" + "    sc.name AS column_name, \n"
        + "    sc.definition AS computation_expression, \n" + "    sc.is_persisted \n" + "FROM \n"
        + "    sys.computed_columns sc \n" + "JOIN \n" + "    sys.objects o ON sc.object_id = o.object_id \n"
        + "JOIN \n" + "    sys.schemas s ON o.schema_id = s.schema_id \n" + "WHERE \n" + "    s.name = ? \n"
        + "    AND o.name = ? \n" + "    AND sc.name = ?";

    /**
     * sql for setting snapshot
     */
    public static final String SET_SNAPSHOT_SQL = "SET TRANSACTION ISOLATION LEVEL SNAPSHOT";

    /**
     * sql for querying table rows
     */
    public static final String QUERY_WITH_LOCK_SQL = "SELECT %s FROM [%s].[%s].[%s] WITH (HOLDLOCK)";

    /**
     * sql for querying current lsn
     */
    public static final String MAX_LSN_SQL = "select sys.fn_cdc_get_max_lsn() AS max_lsn";

    /**
     * sql for querying indexes
     */
    public static final String QUERY_INDEX_SQL =
        "select i.name AS index_name, i.type_desc, i.is_unique, i.is_primary_key, i.has_filter, i.filter_definition, "
            + "     t.name AS table_name,  t.object_id AS object_id"
            + "     from sys.indexes i JOIN  sys.tables t ON t.object_id = i.object_id JOIN "
            + "     sys.schemas s ON t.schema_id = s.schema_id  where i.name IS NOT NULL and s.name = '%s' "
            + "     and i.is_primary_key = 0";

    /**
     * sql for querying cols of index
     */
    public static final String QUERY_INDEX_COL_SQL = "SELECT c.name FROM sys.index_columns ic "
        + "    JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id "
        + "    WHERE ic.object_id = %d AND ic.index_id = (SELECT index_id FROM sys.indexes "
        + "    WHERE object_id = %d AND name = '%s') AND ic.is_included_column = 0 ORDER BY ic.key_ordinal";

    /**
     * sql for querying primary keys
     */
    public static final String QUERY_PRIMARY_KEY_SQL = "SELECT " + "    t.name AS table_name, "
        + "    s.name AS schema_name, " + "    STUFF(( " + "        SELECT ', ' + c.name "
        + "        FROM sys.columns c "
        + "        INNER JOIN sys.index_columns ic ON c.object_id = ic.object_id AND c.column_id = ic.column_id "
        + "        WHERE ic.object_id = t.object_id " + "        AND ic.index_id = pk.index_id "
        + "        ORDER BY ic.key_ordinal " + "        FOR XML PATH('') " + "    ), 1, 2, '') AS pk_columns, "
        + "    pk.name AS pk_name " + "FROM sys.tables t " + "INNER JOIN sys.schemas s ON t.schema_id = s.schema_id "
        + "INNER JOIN sys.indexes pk ON t.object_id = pk.object_id AND pk.is_primary_key = 1 " + "WHERE s.name = ? "
        + "GROUP BY t.name, s.name, pk.name, pk.index_id, t.object_id";

    /**
     * sql for querying foreign keys
     */
    public static final String QUERY_FOREIGN_KEY_SQL = "SELECT \n" + "    fk.name AS fk_name, \n"
        + "    s.name AS schema_name, \n" + "    tp.name AS parent_table,\n" + "    ref.name AS referenced_table,\n"
        + "    STUFF((\n" + "        SELECT ', ' + cp.name\n" + "        FROM sys.columns cp\n"
        + "        INNER JOIN sys.foreign_key_columns fkc2 ON fkc2.parent_object_id = cp.object_id AND fkc2"
        + ".parent_column_id = cp.column_id\n"
        + "        WHERE fkc2.constraint_object_id = fk.object_id\n" + "        ORDER BY fkc2.constraint_column_id\n"
        + "        FOR XML PATH('')\n" + "    ), 1, 2, '') AS parent_columns,\n" + "    STUFF((\n"
        + "        SELECT ', ' + cref.name\n" + "        FROM sys.columns cref\n"
        + "        INNER JOIN sys.foreign_key_columns fkc2 ON fkc2.referenced_object_id = cref.object_id AND fkc2"
        + ".referenced_column_id = cref.column_id\n"
        + "        WHERE fkc2.constraint_object_id = fk.object_id\n" + "        ORDER BY fkc2.constraint_column_id\n"
        + "        FOR XML PATH('')\n" + "    ), 1, 2, '') AS referenced_columns\n" + "FROM sys.foreign_keys fk \n"
        + "INNER JOIN sys.schemas s ON fk.schema_id = s.schema_id\n"
        + "INNER JOIN sys.tables tp ON fk.parent_object_id = tp.object_id\n"
        + "INNER JOIN sys.tables ref ON fk.referenced_object_id = ref.object_id\n" + "WHERE s.name = '%s'\n"
        + "GROUP BY fk.name, s.name, tp.name, ref.name, fk.object_id";

    /**
     * sql for querying unique constraints
     */
    public static final String QUERY_UNIQUE_CONSTRAINT_SQL = "SELECT kc.name AS constraint_name, " + "STUFF(( "
        + "    SELECT ',' + c.name " + "    FROM sys.index_columns ic "
        + "    JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id "
        + "    WHERE ic.object_id = kc.parent_object_id  " + "      AND ic.index_id = kc.unique_index_id "
        + "    ORDER BY ic.key_ordinal " + "    FOR XML PATH('') " + "), 1, 1, '') AS columns "
        + "FROM sys.key_constraints kc " + "JOIN sys.tables t ON kc.parent_object_id = t.object_id "
        + "WHERE kc.type = 'UQ'  " + "  AND t.name = ?  " + "  AND SCHEMA_NAME(t.schema_id) = ? "
        + "GROUP BY kc.name, kc.parent_object_id, kc.unique_index_id";

    /**
     * sql for querying check constraints
     */
    public static final String QUERY_CHECK_CONSTRAINT_SQL = "SELECT cc.name AS constraint_name, cc.definition "
        + "FROM sys.check_constraints cc " + "JOIN sys.tables t ON cc.parent_object_id = t.object_id "
        + "WHERE t.name = ? AND SCHEMA_NAME(t.schema_id) = ?";

    /**
     * sql for querying partition information
     */
    public static final String QUERY_PARTITION_SQL = "SELECT c.name AS partition_column, pf.name AS function_name, "
        + "pf.type_desc AS function_type, pf.fanout AS partition_count, " + "pf.boundary_value_on_right AS is_right "
        + "FROM sys.tables t " + "JOIN sys.indexes i ON t.object_id = i.object_id "
        + "JOIN sys.partition_schemes ps ON i.data_space_id = ps.data_space_id "
        + "JOIN sys.partition_functions pf ON ps.function_id = pf.function_id "
        + "JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id "
        + "JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id "
        + "WHERE t.schema_id = SCHEMA_ID(?) AND t.name = ? AND i.index_id IN (0, 1) AND ic.partition_ordinal > 0";

    /**
     * sql for querying partition boundary
     */
    public static final String QUERY_PARTITION_BOUNDARY_SQL = "SELECT prv.value AS boundary_value "
        + "FROM sys.tables t " + "JOIN sys.indexes i ON t.object_id = i.object_id "
        + "JOIN sys.partition_schemes ps ON i.data_space_id = ps.data_space_id "
        + "JOIN sys.partition_functions pf ON ps.function_id = pf.function_id "
        + "LEFT JOIN sys.partition_range_values prv ON pf.function_id = prv.function_id "
        + "WHERE t.schema_id = SCHEMA_ID(?) AND t.name = ? AND i.index_id IN (0, 1) " + "ORDER BY prv.boundary_id";

    /**
     * sql for querying views
     */
    public static final String QUERY_VIEW_SQL = "SELECT v.name AS name, m.definition AS definition FROM"
        + "    sys.views v JOIN sys.sql_modules m ON v.object_id = m.object_id"
        + "    JOIN sys.schemas s ON v.schema_id = s.schema_id" + "    WHERE s.name = '%s' AND v.is_ms_shipped = 0;";

    /**
     * sql for querying functions
     */
    public static final String QUERY_FUNCTION_SQL = "SELECT o.name AS name, m.definition AS definition"
        + "    FROM sys.objects o JOIN sys.sql_modules m ON o.object_id = m.object_id JOIN sys.schemas s "
        + "    ON o.schema_id = s.schema_id"
        + "    WHERE s.name = '%s' AND o.type IN ('FN', 'IF', 'TF', 'FS', 'FT') AND o.is_ms_shipped = 0;";

    /**
     * sql for querying triggers
     */
    public static final String QUERY_TRIGGER_SQL =
        "SELECT t.name AS name, m.definition AS definition, o.name AS TableName"
            + "    FROM sys.triggers t JOIN sys.sql_modules m ON t.object_id = m.object_id "
            + "    JOIN sys.objects o ON t.parent_id = o.object_id"
            + "    JOIN sys.schemas s ON o.schema_id = s.schema_id"
            + "    WHERE s.name = '%s' AND t.is_ms_shipped = 0;";

    /**
     * sql for querying procedures
     */
    public static final String QUERY_PROCEDURE_SQL = "SELECT o.name AS name, m.definition AS definition"
        + "    FROM sys.objects o JOIN sys.sql_modules m ON o.object_id = m.object_id"
        + "    JOIN sys.schemas s ON o.schema_id = s.schema_id"
        + "    WHERE s.name = '%s' AND o.type = 'P' AND o.is_ms_shipped = 0;";

    /**
     * sql for querying sequences
     */
    public static final String QUERY_SEQUENCE_SQL =
        "SELECT seq.name AS name, seq.start_value AS startValue, seq.increment AS increment,"
            + "    seq.minimum_value AS minValue, seq.maximum_value AS maxValue, seq.is_cycling AS isCycling,"
            + "    seq.cache_size AS cacheSize, seq.current_value AS currentValue, seq.system_type_id AS typeId"
            + "    FROM sys.sequences seq JOIN sys.schemas s ON seq.schema_id = s.schema_id"
            + "    WHERE s.name = '%s';";

    private SqlServerSqlConstants() {
    }
}
