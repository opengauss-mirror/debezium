/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package io.debezium.connector.postgresql.migration;

import java.util.Arrays;
import java.util.List;

/**
 * Description: sqls used to query something from postgresql
 *
 * @author jianghongbo
 * @since 2024/11/8
 */
public class PostgresSqlConstant {
    /**
     * get all schemas from database
     */
    public static final String GETALLSCHEMA = "SELECT pn.oid AS schema_oid, iss.catalog_name, iss.schema_owner,"
            + " iss.schema_name FROM information_schema.schemata iss INNER JOIN pg_namespace pn "
            + " ON pn.nspname = iss.schema_name where iss.schema_name = 'public' OR pn.oid > 16384";

    /**
     * get all tables in schema
     */
    public static final String GETALLTABLE = "select table_name from information_schema.tables where"
            + " table_schema = '%s' and table_type = 'BASE TABLE'";

    /**
     * get table infos
     */
    public static final String GETTABLEINFO = "SELECT column_name as column_name,\n"
            + "   column_default as column_default,\n"
            + "   ordinal_position as ordinal_position,\n"
            + "   data_type as data_type,\n"
            + "   character_maximum_length as character_maximum_length,\n"
            + "   is_nullable as is_nullable,\n"
            + "   numeric_precision as numeric_precision,\n"
            + "   numeric_scale as numeric_scale,\n"
            + "   character_set_name as character_set_name,\n"
            + "   collation_name as collation_name FROM information_schema.COLUMNS WHERE table_schema='%'"
            + "   AND table_name='%' ORDER BY ordinal_position";

    /**
     * create schema statement
     */
    public static final String SCHEMACREATE = "create schema if not exists %s";

    /**
     * switch schema statement
     */
    public static final String SWITCHSCHEMA = "set search_path to %s";

    /**
     * get primary key info
     */
    public static final String GETPKINFO = "SELECT conname AS constraint_name, a.attname AS column_name "
            + "FROM pg_constraint con JOIN pg_attribute a ON a.attnum = ANY(con.conkey) AND a.attrelid = con.conrelid "
            + "    JOIN pg_class c ON c.oid = con.conrelid WHERE con.contype = 'p' AND c.relname = '%s'";

    /**
     * get table index info
     */
    public static final String GETINDEXINFO = "select indexname, indexdef from pg_indexes where tablename = '%s'";

    /**
     * add table unique constraint info
     */
    public static final String ADDUNIQUECONSTRAINT = "alter table %s add constraint %s";

    /**
     * get table unique constraint info
     */
    public static final String GETUNIQUECONSTRAINT = "SELECT conname, pg_get_constraintdef(p.oid) AS "
            + " constraint_definition FROM pg_constraint p JOIN pg_class c ON p.conrelid = c.oid "
            + " JOIN pg_namespace n ON c.relnamespace = n.oid WHERE n.nspname = '%s' and c.relname = '%s' "
            + " AND p.contype = 'u';" ;

    /**
     * create primary key statement header
     */
    public static final String CREATEPK_HEADER = "alter table %s add constraint %s primary key (";

    /**
     * sql tail
     */
    public static final String TAIL = ")";

    /**
     * copy sql statement
     */
    public static final String COPY_SQL = "COPY %s FROM STDIN WITH NULL 'null' CSV "
            + "QUOTE '\"' DELIMITER ',' ESCAPE '\"'";

    /**
     * get all parent tables
     */
    public static final String GET_PARENT_TABLE = "SELECT p.relname AS parent_table_name FROM pg_inherits i JOIN "
            + " pg_class c ON i.inhrelid = c.oid JOIN pg_class p ON i.inhparent = p.oid "
            + " JOIN pg_namespace n ON c.relnamespace = n.oid WHERE n.nspname = '%s' and c.relname = '%s'";

    /**
     * if table have partitions
     */
    public static final String HAVE_PARTITION_SQL = "select relispartition from pg_class c join pg_namespace n ON"
            + " c.relnamespace = n.oid where n.nspname = '%s' and c.relname = '%s'";

    /**
     * get child tables
     */
    public static final String GET_CHILD_TABLE = "SELECT c.relname AS child_table_name FROM pg_inherits i "
            + " JOIN pg_class c ON i.inhrelid = c.oid JOIN pg_class p ON i.inhparent = p.oid "
            + " JOIN pg_namespace n ON c.relnamespace = n.oid WHERE n.nspname = '%s' AND p.relname = '%s'";

    /**
     * get table partition key
     */
    public static final String GET_PARTITION_KEY = "select pg_get_partkeydef(c.oid) from pg_class c "
            + " join pg_namespace n on c.relnamespace = n.oid where n.nspname = '%s' and relname = '%s'";

    /**
     * get table partition expression
     */
    public static final String GET_PARTITION_EXPR = "select pg_get_partkeydef(c.oid), "
            + " pg_get_expr(relpartbound, c.oid) from pg_class c join pg_namespace n on c.relnamespace = n.oid "
            + " where n.nspname = '%s' and relname = '%s'";

    /**
     * sql to create schema sch_debezium and table pg_replica_tables
     */
    public static final List<String> SNAPSHOT_TABLE_CREATE_SQL = Arrays.asList(
            "create schema if not exists sch_debezium;",
            "create table if not exists sch_debezium.pg_replica_tables ("
                    + " id serial,"
                    + " pg_schema_name varchar(100),"
                    + " pg_table_name varchar(100),"
                    + " pg_xlog_location varchar(100),"
                    + " constraint pg_replica_tables_pk1 primary key (id),"
                    + " constraint pg_replica_tables_u1 unique (pg_schema_name, pg_table_name)"
                    + ");");

    /**
     * insert xlogLocation to pg_replica_tables
     */
    public static final String INSERT_REPLICA_TABLES_SQL = "insert into sch_debezium.pg_replica_tables "
            + "(pg_schema_name, pg_table_name, pg_xlog_location) values ('%s', '%s', '%s')"
            + "on duplicate key update pg_xlog_location='%s';";

    /**
     * select xlogLocation from pg_replica_tables
     */
    public static final String SELECT_SNAPSHOT_RECORD_SQL = "select pg_schema_name, pg_table_name, pg_xlog_location"
            + " from sch_debezium.pg_replica_tables;";

    /**
     * postgresql version 9.5.0
     */
    public static final String PG_SERVER_V95 = "9.5.0";

    /**
     * postgresql version 10.0
     */
    public static final String PG_SERVER_V10 = "10.0";

    /**
     * get table some metadata
     */
    public static final String METADATASQL = "select"
            + "    c.relname tableName,"
            + "    c.reltuples tableRows,"
            + "    case"
            + "        when c.reltuples > 0 then pg_table_size(c.oid) / c.reltuples"
            + "        else 0"
            + "    end as avgRowLength "
            + " from"
            + "    pg_class c"
            + "    LEFT JOIN pg_namespace n on n.oid = c.relnamespace"
            + " where"
            + "    n.nspname = '%s' "
            + "    and c.relname = '%s' "
            + " order by"
            + "    c.reltuples asc;";

    /**
     * get role's replication rights
     */
    public static final String REPLICATION_RIGHTS_SQL = "SELECT r.rolcanlogin AS rolcanlogin, "
            + "r.rolreplication AS rolreplication,"
            + " CAST(array_position(ARRAY(SELECT b.rolname"
            + " FROM pg_catalog.pg_auth_members m"
            + " JOIN pg_catalog.pg_roles b ON (m.roleid = b.oid)"
            + " WHERE m.member = r.oid), 'rds_superuser') AS BOOL) IS TRUE AS aws_superuser"
            + ", CAST(array_position(ARRAY(SELECT b.rolname"
            + " FROM pg_catalog.pg_auth_members m"
            + " JOIN pg_catalog.pg_roles b ON (m.roleid = b.oid)"
            + " WHERE m.member = r.oid), 'rdsadmin') AS BOOL) IS TRUE AS aws_admin"
            + ", CAST(array_position(ARRAY(SELECT b.rolname"
            + " FROM pg_catalog.pg_auth_members m"
            + " JOIN pg_catalog.pg_roles b ON (m.roleid = b.oid)"
            + " WHERE m.member = r.oid), 'rdsrepladmin') AS BOOL) IS TRUE AS aws_repladmin"
            + " FROM pg_roles r WHERE r.rolname = current_user";

    /**
     * sql get xlog location before pg10.0
     */
    public static final String GET_XLOG_LOCATION_OLD = "select pg_current_xlog_location()";

    /**
     * sql get xlog location after pg10.0
     */
    public static final String GET_XLOG_LOCATION_NEW = "select * from pg_current_wal_lsn()";

    /**
     * show server version
     */
    public static final String PG_SHOW_SERVER_VERSION = "show server_version";
}
