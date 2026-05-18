/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

package org.full.migration.constants;

/**
 * OracleSqlConstants
 * Oracle database SQL constants definition
 *
 * @since 2025-04-18
 */
public class OracleSqlConstants {
    /**
     * Oracle JDBC URL
     */
    public static final String ORACLE_JDBC_URL = "jdbc:oracle:thin:@//%s:%d/%s?oracle.jdbc.fanEnabled=false";

    /**
     * SQL for querying tables
     */
    public static final String QUERY_TABLE_SQL = """
        SELECT t.table_name AS TableName,t.num_rows AS tableRows,
            CASE WHEN pt.table_name IS NOT NULL THEN 1 ELSE 0 END AS isPartitioned,
            CASE WHEN pk.table_name IS NOT NULL THEN 1 ELSE 0 END AS hasPrimaryKey
        FROM
            user_tables t
            LEFT JOIN user_part_tables pt ON t.table_name = pt.table_name
            LEFT JOIN (
                SELECT DISTINCT table_name FROM user_constraints WHERE constraint_type = 'P'
            ) pk ON t.table_name = pk.table_name
        WHERE t.table_name NOT LIKE 'DR$%'
        ORDER BY t.table_name
    """;

    /**
     * SQL for querying primary keys , sql parameter placeholder is  ? ,this values is oracle table owner
     * table_name,pk_name,pk_columns
     */
    public static final String QUERY_PK_SQL = """
        SELECT ucc.table_name, ucc.constraint_name AS pk_name,
               LISTAGG(ucc.column_name, ', ') WITHIN GROUP (ORDER BY ucc.position) AS pk_columns
        FROM user_cons_columns ucc
        JOIN user_constraints uc ON ucc.constraint_name = uc.constraint_name
        JOIN user_tables ut ON ucc.table_name = ut.table_name
        WHERE ucc.owner = ?  AND uc.constraint_type = 'P' AND ut.table_name NOT LIKE 'DR$%'
        GROUP BY ucc.table_name, ucc.constraint_name
        ORDER BY ucc.table_name, ucc.constraint_name
    """;

    public static final String QUERY_TB_COLUMN_SQL = """
       SELECT  t.table_name,
            LISTAGG(
                CASE
                    WHEN c.data_type = 'XMLTYPE' THEN 'XMLSerialize(CONTENT ' || c.column_name || ' AS CLOB) AS ' || c.column_name
                    ELSE c.column_name
                END,
                ', '
            ) WITHIN GROUP (ORDER BY c.column_id) AS tb_columns
        FROM user_tables t
        JOIN user_tab_cols c ON t.table_name = c.table_name
        WHERE t.table_name NOT LIKE 'DR$%'  AND c.hidden_column = 'NO'
        GROUP BY t.table_name  ORDER BY t.table_name
    """;


    /**
     * SQL for querying views
     */
    public static final String QUERY_VIEW_SQL = """
        SELECT view_name name, text definition FROM user_views ORDER BY view_name
    """;

    /**
     * SQL for querying functions
     */
    public static final String QUERY_FUNCTION_SQL = """
        SELECT object_name name, dbms_metadata.get_ddl('FUNCTION', object_name) AS definition
        FROM user_objects WHERE object_type = 'FUNCTION' ORDER BY object_name
    """;

    /**
     * SQL for querying triggers
     */
    public static final String QUERY_TRIGGER_SQL = """
        SELECT trigger_name name, trigger_type, triggering_event, table_name, status,
               dbms_metadata.get_ddl('TRIGGER', trigger_name) AS definition
        FROM user_triggers ORDER BY trigger_name
    """;

    /**
     * SQL for querying procedures
     */
    public static final String QUERY_PROCEDURE_SQL = """
        SELECT object_name name, dbms_metadata.get_ddl('PROCEDURE', object_name) AS definition
        FROM user_objects WHERE object_type = 'PROCEDURE' ORDER BY object_name
    """;

    /**
     * SQL for querying sequences
     */
    public static final String QUERY_SEQUENCE_SQL = """
        SELECT s.sequence_name AS name, s.min_value, s.max_value, s.increment_by, s.cycle_flag, s.order_flag, s.cache_size, s.last_number 
        FROM user_sequences s 
        WHERE s.sequence_name NOT LIKE 'ISEQ$$%%' ORDER BY s.sequence_name
    """;



    /**
     * SQL for querying foreign keys
     */
    public static final String QUERY_FK_SQL = """
        SELECT a.constraint_name AS fk_name, a.table_name parent_table,
               LISTAGG(DISTINCT b.column_name, ', ') WITHIN GROUP (ORDER BY b.position) AS parent_columns,
               c.table_name AS referenced_table,
               LISTAGG(DISTINCT d.column_name, ', ') WITHIN GROUP (ORDER BY d.position) AS referenced_columns
        FROM user_constraints a
        JOIN user_cons_columns b ON a.constraint_name = b.constraint_name
        JOIN user_constraints c ON a.r_constraint_name = c.constraint_name
        JOIN user_cons_columns d ON c.constraint_name = d.constraint_name
        WHERE a.owner = '%s' AND a.constraint_type = 'R'
        GROUP BY a.constraint_name, a.table_name, c.table_name
        ORDER BY a.table_name, a.constraint_name
    """;

    /**
     * SQL for querying indexes
     */
    public static final String QUERY_INDEX_SQL = """
        SELECT
            i.index_name,  i.index_type AS type_desc,
            'false' AS has_filter,  NULL AS filter_definition,
            CASE WHEN i.uniqueness = 'UNIQUE' THEN 'true' ELSE 'false' END AS is_unique,
            CASE WHEN c.constraint_type = 'P' THEN 'true' ELSE 'false' END AS is_primary_key,
            CASE WHEN e.column_expression IS NOT NULL THEN 'true' ELSE 'false' END AS has_expression,
            CASE WHEN e.column_expression IS NOT NULL THEN e.column_expression ELSE null END AS index_expression,
            i.table_name, i.tablespace_name, i.status
        FROM user_indexes i
        LEFT JOIN user_constraints c ON i.index_name = c.index_name AND c.constraint_type = 'P'
        LEFT JOIN user_ind_expressions e ON i.index_name = e.index_name
        WHERE i.table_name NOT LIKE 'BIN$%' and i.table_name NOT LIKE 'DR$%'
        ORDER BY i.table_name, i.index_name
    """;

    /**
     * SQL for querying index columns
     */
    public static final String QUERY_INDEX_DDL_SQL = """
        SELECT DBMS_METADATA.GET_DDL('INDEX', ?) index_ddl FROM DUAL
    """;

    /**
     * PL/SQL for setting DBMS_METADATA transform parameters
     */
    public static final String SET_METADATA_TRANSFORM_PARAMS = """
        BEGIN
          DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'EMIT_SCHEMA', FALSE);
          DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'SEGMENT_ATTRIBUTES', FALSE);
          DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'STORAGE', FALSE);
          DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'TABLESPACE', FALSE);
          DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'PRETTY', TRUE);
          DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'SQLTERMINATOR', TRUE);
        END;
    """;

    /**
     * SQL for querying generate column define : column_name, data_type,DATA_DEFAULT as expression, virtual_column
     */
    public static final String QUERY_GENERATE_DEFINE_SQL = """
        SELECT COLUMN_NAME,DATA_TYPE, DATA_DEFAULT AS EXPRESSION, VIRTUAL_COLUMN FROM USER_TAB_COLS
        WHERE HIDDEN_COLUMN = 'NO' AND TABLE_NAME = ? AND COLUMN_NAME = ? 
    """;
    
    /**
     * SQL for querying unique constraints
     */
    public static final String QUERY_UNIQUE_CONSTRAINT_SQL = """
        SELECT ucc.table_name,  ucc.constraint_name, LISTAGG(ucc.column_name, ', ') WITHIN GROUP (ORDER BY ucc.position) AS columns
        FROM  user_cons_columns ucc
        JOIN user_constraints uc ON uc.constraint_name = ucc.constraint_name AND uc.constraint_type = 'U'
        WHERE ucc.table_name not like '%BIN$%' and ucc.table_name not like '%DR$%'
        GROUP BY ucc.table_name, ucc.constraint_name
        ORDER BY ucc.table_name, ucc.constraint_name
    """;
    
    /**
     * SQL for querying check constraints
     */
    public static final String QUERY_CHECK_CONSTRAINT_SQL = """
        SELECT table_name, constraint_name, search_condition_vc AS definition
        FROM user_constraints WHERE constraint_type = 'C' and 
        table_name not like '%BIN$%' and table_name not like '%DR$%'
        ORDER BY table_name, constraint_name
    """;

    /**
     * SQL for querying partition metadata (合并多个查询为一个，减少数据库访问)
     */
    public static final String QUERY_PARTITION_METADATA_SQL = """
        SELECT 
            T.PARTITIONED,
            PT.PARTITIONING_TYPE,
            PT.INTERVAL,
            CASE WHEN pt.subpartitioning_type IS NOT NULL THEN 'YES' ELSE 'NO' END as subpartitioned,
            pt.subpartitioning_type
        FROM USER_TABLES T
        LEFT JOIN USER_PART_TABLES PT ON T.TABLE_NAME = PT.TABLE_NAME
        WHERE T.TABLE_NAME = ?
    """;

    /**
     * SQL for checking if a table is a partition child table
     */
    public static final String QUERY_PARTITION_CHILD_TABLE_SQL = """
        SELECT COUNT(*) FROM user_tab_partitions WHERE table_name = ?
    """;

    /**
     * SQL for querying partition key
     */
    public static final String QUERY_PARTITION_KEY_SQL = """
        SELECT partition_name, high_value, tablespace_name FROM user_tab_partitions
        WHERE table_name = ? ORDER BY partition_position
    """;

    /**
     * SQL for querying partition columns
     */
    public static final String QUERY_PARTITION_COLUMNS_SQL = """
        SELECT column_name  FROM user_part_key_columns WHERE name = ? ORDER BY column_position
    """;

    /**
     * SQL for querying subpartition columns
     */
    public static final String QUERY_SUBPARTITION_COLUMNS_SQL = """
        SELECT column_name
            FROM user_subpart_key_columns
            WHERE name = ?
        ORDER BY column_position
    """;

    /**
     * SQL for querying subpartitions
     */
    public static final String QUERY_SUBPARTITIONS_SQL = """
        SELECT SUBPARTITION_NAME, HIGH_VALUE, TABLESPACE_NAME
            FROM USER_TAB_SUBPARTITIONS
            WHERE TABLE_NAME = ? AND PARTITION_NAME = ?
        ORDER BY SUBPARTITION_POSITION
    """;

    /**
     * SQL for querying column char used
     */
    public static final String QUERY_COLUMN_CHAR_USED_SQL = """
        SELECT column_name, char_used FROM user_tab_columns WHERE table_name = ?
    """;

    /**
     * SQL for querying column comment
     */
    public static final String QUERY_COLUMN_COMMENT_SQL = """
        SELECT column_name, comments FROM user_col_comments WHERE table_name = ?
    """;
    
    /**
     * SQL for querying table comment
     */
    public static final String QUERY_TABLE_COMMENT_SQL = """
        SELECT COMMENTS FROM user_tab_comments WHERE TABLE_NAME = ?
    """;

    /**
     * SQL for checking if a table column is auto-incremented and identity_column <br>
     * condition : table_name = ? AND column_name = ?
     */
    public static final String QUERY_IS_AUTO_INCREMENT_SQL = """
        SELECT CASE WHEN identity_column = 'YES' THEN 1 ELSE 0 END AS is_identity_column FROM user_tab_columns  
        WHERE table_name = ? AND column_name = ?    
    """;

    /**
     * SQL for querying auto-increment columns in a table this only works for identity columns
     */
    public static final String QUERY_AUTO_INCREMENT_COLUMNS_SQL = """
         SELECT table_name, column_name FROM all_tab_columns WHERE owner = ? AND identity_column = 'YES' ORDER BY column_id
    """;

    /**
     * SQL for querying max value of a table column
     */
    public static final String QUERY_TABLE_COLUMN_MAX_VALUE_SQL = """
        SELECT max(%s) AS max_value FROM %s
    """;

}
