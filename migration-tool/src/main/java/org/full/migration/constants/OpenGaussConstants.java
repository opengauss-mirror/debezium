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
 * OpenGaussConstants
 *
 * @since 2025-05-12
 */
public final class OpenGaussConstants {
    /**
     * sql for querying composite types
     */
    public static final String QUERY_COMPOSITE_TYPE_SQL = "SELECT t.typname as type_name, "
            + "('CREATE TYPE ' || n.nspname || '.' || t.typname || ' AS (' || "
            + "(SELECT string_agg(a.attname || ' ' || "
            + "pg_catalog.format_type(a.atttypid, a.atttypmod), ', ' ORDER BY a.attnum) "
            + "FROM pg_catalog.pg_attribute a "
            + "WHERE a.attrelid = t.typrelid AND a.attnum > 0 AND NOT a.attisdropped) || ')') as type_definition, "
            + "pg_catalog.obj_description(t.oid, 'pg_type') as type_comment "
            + "FROM pg_catalog.pg_type t "
            + "JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace "
            + "WHERE n.nspname = ? AND t.typtype = 'c' "
            + "AND EXISTS (SELECT 1 FROM pg_catalog.pg_class c WHERE c.reltype = t.oid AND c.relkind = 'c')";

    /**
     * sql for querying composite types
     */
    public static final String QUERY_ENUM_TYPE_SQL = "SELECT t.typname as enum_name, "
            + "pg_catalog.array_agg(e.enumlabel ORDER BY e.enumsortorder) as enum_values, "
            + "pg_catalog.obj_description(t.oid, 'pg_type') as enum_comment "
            + "FROM pg_catalog.pg_type t "
            + "JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace "
            + "JOIN pg_catalog.pg_enum e ON e.enumtypid = t.oid "
            + "WHERE n.nspname = ? AND t.typtype = 'e' "
            + "GROUP BY t.typname, n.nspname, t.oid";

    /**
     * sql for querying composite types
     */
    public static final String QUERY_DOMAIN_TYPE_SQL = "SELECT d.domain_name, "
            + "pg_catalog.format_type(t.typbasetype, t.typtypmod) as base_type, "
            + "t.typnotnull as not_null, "
            + "pg_catalog.pg_get_constraintdef(c.oid, true) as check_constraint, "
            + "pg_catalog.obj_description(t.oid, 'pg_type') as domain_comment "
            + "FROM pg_catalog.pg_type t "
            + "JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace "
            + "JOIN information_schema.domains d ON d.domain_name = t.typname AND d.domain_schema = n.nspname "
            + "LEFT JOIN pg_catalog.pg_constraint c ON c.contypid = t.oid "
            + "WHERE n.nspname = ? AND t.typtype = 'd'";

    /**
     * sql for querying all tables
     */
    public static final String QUERY_TABLE_SQL = "WITH TableStats AS (\n"
            + "SELECT\n"
            + "    t.tablename AS tableName,\n"
            + "    c.relname AS relName,\n"
            + "    pg_stat_user_tables.n_live_tup AS tableRows,\n"
            + "    pg_table_size(c.oid) / 1024.0 AS totalTableSize,\n"
            + "    CASE\n"
            + "        WHEN c.parttype = 'n' THEN 0\n"
            + "        ELSE 1\n"
            + "    END AS isPartitioned,\n"
            + "    CASE\n"
            +        "  WHEN c.parttype = 's' THEN 1\n"
            +        "  ELSE 0\n"
            + "    END AS isSubPartitioned,\n"
            + "    CASE\n"
            + "        WHEN EXISTS (\n"
            + "            SELECT 1\n"
            + "            FROM pg_index i\n"
            + "            JOIN pg_class ic ON i.indexrelid = ic.oid\n"
            + "            WHERE i.indrelid = c.oid\n"
            + "            AND i.indisprimary\n"
            + "        ) THEN 1\n"
            + "        ELSE 0\n"
            + "    END AS hasPrimaryKey,\n"
            + "    COALESCE(('segment=on' = ANY(c.reloptions))::int, 0) AS has_segment_on\n"
            + "FROM\n"
            + "    pg_catalog.pg_tables t\n"
            + "JOIN\n"
            + "    pg_catalog.pg_namespace n ON t.schemaname = n.nspname\n"
            + "JOIN\n"
            + "    pg_catalog.pg_class c ON t.tablename = c.relname AND c.relnamespace = n.oid\n"
            + "LEFT JOIN\n"
            + "    pg_stat_user_tables ON pg_stat_user_tables.relid = c.oid\n"
            + "WHERE\n"
            + "    t.schemaname = '%s' -- 只查询特定schema\n"
            + ")\n"
            + "SELECT\n"
            + "    tableName,\n"
            + "    tableRows,\n"
            + "    totalTableSize,\n"
            + "    isPartitioned,\n"
            + "    isSubPartitioned,\n"
            + "    hasPrimaryKey,\n"
            + "    has_segment_on\n"
            + "FROM\n"
            + "    TableStats\n"
            + "ORDER BY\n"
            + "    tableRows ASC;";

    /**
     * sql for querying all tables with sql_compatibility = B
     */
    public static final String QUERY_TABLE_SQL_B = "WITH tablestats AS (\n"
            + "SELECT\n"
            + "    t.tablename AS tableName,\n"
            + "    c.relname AS relName,\n"
            + "    pg_stat_user_tables.n_live_tup AS tableRows,\n"
            + "    pg_table_size(c.oid) / 1024.0 AS totalTableSize,\n"
            + "    CASE\n"
            + "        WHEN c.parttype = 'n' THEN 0\n"
            + "        ELSE 1\n"
            + "    END AS isPartitioned,\n"
            + "    CASE\n"
            +        "  WHEN c.parttype = 's' THEN 1\n"
            +        "  ELSE 0\n"
            + "    END AS isSubPartitioned,\n"
            + "    CASE\n"
            + "        WHEN EXISTS (\n"
            + "            SELECT 1\n"
            + "            FROM pg_index i\n"
            + "            JOIN pg_class ic ON i.indexrelid = ic.oid\n"
            + "            WHERE i.indrelid = c.oid\n"
            + "            AND i.indisprimary\n"
            + "        ) THEN 1\n"
            + "        ELSE 0\n"
            + "    END AS hasPrimaryKey,\n"
            + "    COALESCE(('segment=on' = ANY(c.reloptions))::int, 0) AS has_segment_on\n"
            + "FROM\n"
            + "    pg_catalog.pg_tables t\n"
            + "JOIN\n"
            + "    pg_catalog.pg_namespace n ON t.schemaname = n.nspname\n"
            + "JOIN\n"
            + "    pg_catalog.pg_class c ON t.tablename = c.relname AND c.relnamespace = n.oid\n"
            + "LEFT JOIN\n"
            + "    pg_stat_user_tables ON pg_stat_user_tables.relid = c.oid\n"
            + "WHERE\n"
            + "    t.schemaname = '%s' -- 只查询特定schema\n"
            + ")\n"
            + "SELECT\n"
            + "    tableName,\n"
            + "    tableRows,\n"
            + "    totalTableSize,\n"
            + "    isPartitioned,\n"
            + "    isSubPartitioned,\n"
            + "    hasPrimaryKey,\n"
            + "    has_segment_on\n"
            + "FROM\n"
            + "    tablestats\n"
            + "ORDER BY\n"
            + "    tableRows ASC;";

    /**
     * sql for check column is generate
     */
    public static final String Check_COLUMN_IS_GENERATE = "SELECT\n"
            + "    is_generated = 'ALWAYS' AS is_generated\n"
            + "    FROM\n"
            + "    information_schema.columns\n"
            + "WHERE\n"
            + "    table_schema = ?\n"
            + "    AND table_name = ?\n"
            + "    AND column_name = ?;";

    /**
     * sql for querying generate define
     */
    public static final String QUERY_GENERATE_DEFINE_SQL = "SELECT\n"
            + "    a.attname AS column_name,\n"
            + "    pg_get_expr(ad.adbin, ad.adrelid) AS computation_expression,\n"
            + "    (ad.adbin IS NOT NULL) AS is_stored\n"
            + "FROM\n"
            + "    pg_catalog.pg_attribute a\n"
            + "JOIN\n"
            + "    pg_catalog.pg_class c ON a.attrelid = c.oid\n"
            + "JOIN\n"
            + "    pg_catalog.pg_namespace n ON c.relnamespace = n.oid\n"
            + "LEFT JOIN\n"
            + "    pg_catalog.pg_attrdef ad ON a.attnum = ad.adnum AND a.attrelid = ad.adrelid\n"
            + "WHERE\n"
            + "    n.nspname = ? \n"
            + "    AND c.relname = ? \n"
            + "    AND a.attname = ? \n"
            + "    AND c.relkind = 'r';\n";

    /**
     * sql for setting snapshot
     */
    public static final String SET_SNAPSHOT_SQL = "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;";

    /**
     * sql for setting table snapshot
     */
    public static final String SET_TABLE_SNAPSHOT_SQL = "BEGIN; LOCK TABLE %s.%s IN SHARE MODE;";

    /**
     * sql get xlog location
     */
    public static final String GET_XLOG_LOCATION_OLD = "select pg_current_xlog_location() AS max_lsn";

    /**
     * sql for querying not patition table rows
     */
    public static final String QUERY_FROM_PARENT_SQL = "SELECT %s FROM ONLY %s.%s";

    /**
     * sql for querying patition table rows
     */
    public static final String QUERY_FROM_TABLE_SQL = "SELECT %s FROM %s.%s";

    /**
     * sql for querying indexes
     */
    public static final String QUERY_INDEX_SQL = "SELECT DISTINCT\n" +
            "  i.relname AS index_name,\n" +
            "  am.amname AS type_desc,\n" +
            "  CASE\n" +
            "      WHEN idx.indisunique THEN true\n" +
            "      ELSE false\n" +
            "  END AS is_unique,\n" +
            "  CASE\n" +
            "      WHEN idx.indisprimary THEN true\n" +
            "      ELSE false\n" +
            "  END AS is_primary_key,\n" +
            "  CASE\n" +
            "      WHEN idx.indpred IS NOT NULL THEN true\n" +
            "      ELSE false\n" +
            "  END AS has_filter,\n" +
            "  CASE\n" +
            "      WHEN idx.indpred IS NOT NULL THEN pg_get_expr(idx.indpred, idx.indrelid)\n" +
            "      ELSE ''\n" +
            "  END AS filter_definition,\n" +
            "  CASE\n" +
            "      WHEN idx.indexprs IS NOT NULL THEN pg_get_expr(idx.indexprs, idx.indrelid)\n" +
            "      ELSE ''\n" +
            "  END AS index_expression,\n" +
            "  t.relname AS table_name,\n" +
            "  t.oid AS object_id,\n" +
            "  -- 判断索引作用域：LOCAL / GLOBAL / NON-PARTITIONED\n" +
            "  CASE\n" +
            "      WHEN pt.parttype = 'p' THEN\n" +
            "          CASE\n" +
            "              WHEN EXISTS (\n" +
            "                  SELECT 1 \n" +
            "                  FROM pg_partition ip \n" +
            "                  WHERE ip.parentid = i.oid\n" +
            "              ) THEN 'LOCAL'\n" +
            "              ELSE 'GLOBAL'\n" +
            "          END\n" +
            "      ELSE 'NON-PARTITIONED'\n" +
            "  END AS index_type\n" +
            "FROM\n" +
            "  pg_index idx\n" +
            "JOIN\n" +
            "  pg_class i ON i.oid = idx.indexrelid\n" +
            "JOIN\n" +
            "  pg_class t ON t.oid = idx.indrelid\n" +
            "JOIN\n" +
            "  pg_namespace n ON n.oid = t.relnamespace\n" +
            "JOIN\n" +
            "  pg_am am ON i.relam = am.oid\n" +
            "LEFT JOIN\n" +
            "  pg_partition pt ON pt.parentid = t.oid AND pt.parttype = 'p'\n" +
            "WHERE\n" +
            "  n.nspname = '%s'\n" +
            "  AND (i.relkind = 'i' OR i.relkind = 'I')\n" +
            "ORDER BY\n" +
            "  t.relname, i.relname;";

    /**
     * sql for querying cols of index
     */
    public static final String QUERY_INDEX_COL_SQL = "SELECT a.attname AS column_name\n"
        + "FROM pg_index i\n"
        + "JOIN pg_attribute a ON a.attnum = ANY(i.indkey)\n"
        + "JOIN pg_class c ON c.oid = i.indrelid\n"
        + "JOIN pg_class idx ON idx.oid = i.indexrelid\n"
        + "WHERE c.oid = %d\n"
        + "  AND idx.relname = '%s'\n"
        + "  AND a.attnum > 0\n"
        + "  AND a.attrelid = c.oid\n"
        + "ORDER BY a.attnum;";

    /**
     * sql for querying whether the unique index is unique constraint
     */
    public static final String QUERY_CONSTRAINTS_SQL = "SELECT COUNT(*) AS constraint_count\n" +
            "FROM pg_constraint con\n" +
            "JOIN pg_class c ON con.conrelid = c.oid\n" +
            "JOIN pg_namespace n ON c.relnamespace = n.oid\n" +
            "WHERE n.nspname = '%s'     \n" +
            "  AND con.conname = '%s'            \n" +
            "  AND con.contype = 'u';";

    /**
     * sql for querying primary keys
     */
    public static final String QUERY_PRIMARY_KEY_SQL = "SELECT\n"
        + "    t.table_name,\n"
        + "    t.table_schema,\n"
        + "    STRING_AGG(c.column_name, ', ') AS pk_columns,\n"
        + "    pk.constraint_name AS pk_name\n"
        + "FROM\n"
        + "    information_schema.tables t\n"
        + "JOIN\n"
        + "    information_schema.table_constraints pk\n"
        + "    ON t.table_name = pk.table_name\n"
        + "    AND t.table_schema = pk.table_schema\n"
        + "    AND pk.constraint_type = 'PRIMARY KEY'\n"
        + "JOIN\n"
        + "    information_schema.key_column_usage kcu\n"
        + "    ON pk.constraint_name = kcu.constraint_name\n"
        + "    AND pk.table_schema = kcu.table_schema\n"
        + "    AND pk.table_name = kcu.table_name\n"
        + "JOIN\n"
        + "    information_schema.columns c\n"
        + "    ON c.table_name = kcu.table_name\n"
        + "    AND c.table_schema = kcu.table_schema\n"
        + "    AND c.column_name = kcu.column_name\n"
        + "WHERE\n"
        + "    t.table_schema = ?\n"
        + "GROUP BY\n"
        + "    t.table_name, t.table_schema, pk.constraint_name\n"
        + "ORDER BY\n"
        + "    t.table_name;";

    /**
     * sql for querying foreign keys
     */
    public static final String QUERY_FOREIGN_KEY_SQL = " SELECT\n"
        + "    fk.constraint_name AS fk_name,\n"
        + "    fk.table_schema AS schema_name,\n"
        + "    tp.table_name AS parent_table,\n"
        + "    ref.table_name AS referenced_table,\n"
        + "    kcu.column_name AS parent_columns,\n"
        + "    ref_kcu.column_name AS referenced_columns\n"
        + "FROM\n"
        + "    information_schema.table_constraints fk\n"
        + "JOIN\n"
        + "    information_schema.key_column_usage kcu\n"
        + "    ON fk.constraint_name = kcu.constraint_name\n"
        + "    AND fk.table_schema = kcu.table_schema\n"
        + "JOIN\n"
        + "    information_schema.referential_constraints rc\n"
        + "    ON fk.constraint_name = rc.constraint_name\n"
        + "    AND fk.table_schema = rc.constraint_schema\n"
        + "JOIN\n"
        + "    information_schema.key_column_usage ref_kcu\n"
        + "    ON ref_kcu.constraint_name = rc.unique_constraint_name\n"
        + "    AND ref_kcu.table_schema = rc.unique_constraint_schema\n"
        + "JOIN\n"
        + "    information_schema.tables tp\n"
        + "    ON tp.table_name = kcu.table_name\n"
        + "    AND tp.table_schema = kcu.table_schema\n"
        + "JOIN\n"
        + "    information_schema.tables ref\n"
        + "    ON ref.table_name = ref_kcu.table_name\n"
        + "    AND ref.table_schema = ref_kcu.table_schema\n"
        + "WHERE\n"
        + "    fk.constraint_type = 'FOREIGN KEY'\n"
        + "    AND fk.table_schema = '%s';\n";

    /**
     * sql for querying unique constraints
     */
    public static final String QUERY_UNIQUE_CONSTRAINT_SQL = "SELECT\n"
        + "  n.nspname AS schema_name,\n"
        + "  t.relname AS table_name,\n"
        + "  c.conname AS constraint_name,\n"
        + "  string_agg(a.attname, ', ') AS columns\n"
        + "FROM\n"
        + "  pg_constraint c\n"
        + "  JOIN pg_attribute a ON a.attnum = ANY(c.conkey) AND a.attrelid = c.conrelid\n"
        + "  JOIN pg_class t ON t.oid = c.conrelid\n"
        + "  JOIN pg_namespace n ON n.oid = t.relnamespace\n"
        + "WHERE\n"
        + "  c.contype = 'u'\n"
        + "  AND n.nspname = ?\n"
        + "GROUP BY\n"
        + "  n.nspname, t.relname, c.conname\n"
        + "ORDER BY\n"
        + "  schema_name, table_name, constraint_name;";

    /**
     * sql for querying check constraints
     */
    public static final String QUERY_CHECK_CONSTRAINT_SQL = "SELECT\n"
        + "    n.nspname AS schema_name,\n"
        + "    t.relname AS table_name,\n"
        + "    c.conname AS constraint_name,\n"
        + "    REGEXP_REPLACE(pg_get_constraintdef(c.oid), '^CHECK \\((.*)\\)$', '\\1') AS definition\n"
        + "FROM\n"
        + "    pg_constraint c\n"
        + "    JOIN pg_class t ON t.oid = c.conrelid\n"
        + "    JOIN pg_namespace n ON n.oid = t.relnamespace\n"
        + "WHERE\n"
        + "    c.contype = 'c'\n"
        + "    AND n.nspname = ?\n"
        + "ORDER BY n.nspname, t.relname, c.conname;";

    /**
     * get all parent tables
     */
    public static final String GET_PARENT_TABLE = "SELECT p.relname AS parent_table_name FROM pg_inherits i JOIN "
            + " pg_class c ON i.inhrelid = c.oid JOIN pg_class p ON i.inhparent = p.oid "
            + " JOIN pg_namespace n ON c.relnamespace = n.oid WHERE n.nspname = '%s' and c.relname = '%s'"
            + " ORDER BY i.inhseqno";

    /**
     * get all child tables
     */
    public static final String GET_CHILD_TABLE = "SELECT c.relname AS child_table_name FROM pg_inherits i "
            + " JOIN pg_class c ON i.inhrelid = c.oid JOIN pg_class p ON i.inhparent = p.oid "
            + " JOIN pg_namespace n ON c.relnamespace = n.oid WHERE n.nspname = '%s' AND p.relname = '%s'";

    public static final String GET_PARENT_PARTITION_INFO = " SELECT\n"
        + "p.parentid AS parentid,\n"
        + "p.relname AS partname,\n"
        + "a.attname AS partcolumn,\n"
        + "p.partstrategy AS partstrategy,\n"
        + "CASE p.partstrategy\n"
        + "   WHEN 'r' THEN 'RANGE'\n"
        + "    WHEN 'i' THEN 'RANGE'\n"
        + "  WHEN 'l' THEN 'LIST'\n"
        + "   WHEN 'h' THEN 'HASH'\n"
        + "END AS parttype,\n"
        + "p.boundaries AS boundaries,\n"
        + "p.interval AS interval\n"
        + " FROM pg_partition p\n"
        + "JOIN pg_class c ON p.parentid = c.oid\n"
        + "JOIN pg_namespace n ON c.relnamespace = n.oid\n"
        + "JOIN pg_attribute a ON a.attrelid = c.oid\n"
        + " AND a.attnum = ANY(ARRAY(SELECT unnest(p.partkey::int[])))\n"
        + "WHERE n.nspname = ? and p.relname = ? ;\n";

    /**
     * sql for querying partition table partition information
     */
    public static final String GET_FIRST_PARTITION_INFO = "  SELECT\n"
        + "p.oid AS firstOid,\n"
        + "p.relname AS partname,\n"
        + "p.partstrategy AS partstrategy,\n"
        + "CASE p.partstrategy\n"
        + "   WHEN 'r' THEN 'RANGE'\n"
        + "    WHEN 'i' THEN 'RANGE'\n"
        + "  WHEN 'l' THEN 'LIST'\n"
        + "   WHEN 'h' THEN 'HASH'\n"
        + "END AS parttype,\n"
        + "p.boundaries AS boundaries,\n"
        + "p.interval AS interval\n"
        + " FROM pg_partition p\n"
        + "WHERE p.parentid=? and p.relname!=?;";

    /**
     * get subpartition table's secondary partition information
     */
    public static final String GET_SECONDARY_PARTITION_INFO = "select p.oid AS subOid,"
        + "p.relname AS partname,\n"
        + "p.partstrategy AS partstrategy,\n"
        + "CASE p.partstrategy\n"
        + "   WHEN 'r' THEN 'RANGE'\n"
        + "   WHEN 'i' THEN 'RANGE'\n"
        + "   WHEN 'l' THEN 'LIST'\n"
        + "   WHEN 'h' THEN 'HASH'\n"
        + "END AS parttype,\n"
        + "p.boundaries AS boundaries,\n"
        + "p.interval AS interval\n"
        + " FROM pg_partition p\n"
        + " WHERE p.parentid=?;";

    /**
     * get table secondary partition column
     */
    public static final String GET_SECONDARY_PARTITION_COLUMN = "SELECT DISTINCT\n"
        + " a.attname as column \n"
        + "FROM pg_partition p\n"
        + "JOIN pg_class c ON p.parentid = c.oid\n"
        + "JOIN pg_namespace n ON c.relnamespace = n.oid\n"
        + "JOIN pg_attribute a ON a.attrelid = c.oid\n"
        + " AND a.attnum = ANY(ARRAY(SELECT unnest(p.partkey::int[])))\n"
        + "WHERE n.nspname = ? AND c.relname = ? AND c.relname != p.relname;\n";

    /**
     * sql for querying all views: view, materialized view
     */
    public static final String QUERY_ALL_VIEW_SQL = """
            SELECT
                n.nspname AS schema,
                c.relname AS name,
            	c.relkind AS view_type,
            	mv.ivm AS isIncremental,
                pg_get_viewdef(c.oid, true) AS definition
            FROM pg_class c
            JOIN pg_namespace n ON c.relnamespace = n.oid
            LEFT JOIN gs_matview mv ON c.oid = mv.matviewid
            WHERE n.nspname = ?
                AND c.relkind IN ('v', 'm');
            """;

    /**
     * sql for querying functions
     */
    public static final String QUERY_FUNCTION_SQL =
        "SELECT p.proname AS name, (pg_get_functiondef(p.oid)).definition AS definition "
            + "FROM pg_proc p "
            + "JOIN pg_namespace n ON p.pronamespace = n.oid "
            + "WHERE n.nspname = '%s' AND p.prokind = 'f' "
            + "AND n.nspname NOT IN ('pg_catalog', 'information_schema');";

    /**
     * sql for querying triggers
     */
    public static final String QUERY_TRIGGER_SQL =
        "SELECT t.tgname AS name, pg_get_triggerdef(t.oid) AS definition, "
            + "c.relname AS TableName "
            + "FROM pg_trigger t "
            + "JOIN pg_class c ON t.tgrelid = c.oid "
            + "JOIN pg_namespace n ON c.relnamespace = n.oid "
            + "WHERE n.nspname = '%s' AND NOT t.tgisinternal "
            + "AND n.nspname NOT IN ('pg_catalog', 'information_schema');";

    /**
     * sql for querying procedures
     */
    public static final String QUERY_PROCEDURE_SQL = """
            SELECT p.proname AS name,
                   p.oid AS oid
            FROM pg_proc p
            JOIN pg_namespace n ON p.pronamespace = n.oid
            WHERE n.nspname = ?
              AND p.prokind = 'p'  -- 'p'表示procedure(存储过程)
              AND n.nspname NOT IN ('pg_catalog', 'information_schema');
            """;

    /**
     * sql for querying procedure definition
     */
    public static final String SELECT_PG_GET_FUNCTION_DEF = "select definition from pg_get_functiondef(?);";

    /**
     * sql for querying all sequences
     */
    public static final String QUERY_ALL_SEQUENCES_SQL = """
            SELECT c.relname, c.relkind
            FROM pg_class c
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE n.nspname = ? AND (c.relkind = 'S' OR c.relkind = 'L');
            """;

    public static final String QUERY_SEQUENCE_USAGE_SQL = """
            WITH sequence_usage AS (
                SELECT
                    n.nspname AS schema_name,
                    c.relname AS table_name,
                    a.attname AS column_name,
                    regexp_replace(
                        pg_get_expr(d.adbin, d.adrelid),
                        E'.*nextval\\\\(''([^'']+)''.*',
                        E'\\\\1'
                    ) AS sequence_name
                FROM pg_class c
                JOIN pg_namespace n ON c.relnamespace = n.oid
                JOIN pg_attribute a ON a.attrelid = c.oid
                JOIN pg_attrdef d ON d.adrelid = c.oid AND d.adnum = a.attnum
                WHERE c.relkind IN ('r', 'p')
                    AND a.attnum > 0
                    AND NOT a.attisdropped
                    AND pg_get_expr(d.adbin, d.adrelid) LIKE '%nextval%'
                    AND n.nspname = ?  -- 指定schema
            )
            SELECT
                schema_name,
                table_name,
                column_name,
            	sequence_name
            FROM sequence_usage
            ORDER BY table_name, column_name;
            """;

    /**
     * sql for querying sequence info
     */
    public static final String SELECT_SEQUENCE_SQL = """
            SELECT
                sequence_name,
                last_value,
                start_value,
                increment_by,
                max_value,
                min_value,
                cache_value,
                is_cycled
            FROM
                %s.%s
            """;

    /**
     * sql for creating logical replication slot
     */
    public static final String PG_CREATE_LOGICAL_REPLICATION_SLOT = "SELECT * FROM pg_create_logical_replication_slot('%s', '%s');";

    /**
     * sql for getting logical replication slot
     */
    public static final String PG_GET_LOGICAL_REPLICATION_SLOT = "SELECT * from  pg_get_replication_slots() WHERE slot_name = '%s';";

    /**
     * sql for droping logical replication slot
     */
    public static final String PG_DROP_LOGICAL_REPLICATION_SLOT = "SELECT * from pg_drop_replication_slot('%s');";

    /**
     * sql for setting table replica idntity full
     */
    public static final String PG_SET_TABLE_REPLICA_IDNTITY_FULL = "alter table %s.%s replica identity full;";

    /**
     * sql for obtaining publication
     */
    public static final String SELECT_PUBLICATION = "SELECT COUNT(1) FROM pg_publication WHERE pubname = 'dbz_publication'";

    /**
     * sql for droping publication
     */
    public static final String DROP_PUBLICATION = "DROP PUBLICATION dbz_publication;";

    /**
     * sql for creating publication
     */
    public static final String CREATE_PUBLICATION = "CREATE PUBLICATION dbz_publication FOR TABLE %s;";

    /**
     * sql for querying table definition
     */
    public static final String SELECT_PG_GET_TABLE_DEF = "SELECT pg_get_tabledef(?);";

    /**
     * sql for querying sql_compatibility
     */
    public static final String SHOW_SQL_COMPATIBILITY = "SHOW sql_compatibility;";

    /**
     * Show dolphin.sql_mode, support openGauss
     */
    public static final String OPENGAUSS_SHOW_DOLPHIN_SQL_MODE = "show dolphin.sql_mode;";

    /**
     * Set dolphin.sql_mode, support openGauss
     */
    public static final String OPENGAUSS_SET_DOLPHIN_SQL_MODE_MODEL = "set dolphin.sql_mode = '%s';";

    private OpenGaussConstants() {
    }
}
