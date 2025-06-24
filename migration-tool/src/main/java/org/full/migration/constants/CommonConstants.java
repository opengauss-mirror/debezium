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

import java.util.Arrays;
import java.util.List;

/**
 * CommonConstants
 *
 * @since 2025-04-18
 */
public class CommonConstants {
    /**
     * TASK_TYPE
     */
    public static final String TASK_TYPE = "--start";

    /**
     * SOURCE_DATABASE
     */
    public static final String SOURCE_DATABASE = "--source";

    /**
     * CONFIG_PATH
     */
    public static final String CONFIG_PATH = "--config";

    /**
     * DELIMITER
     */
    public static final String DELIMITER = ",";

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
     * drop replica schema information
     */
    public static final String DROP_REPLICA_SCHEMA_SQL = "drop schema if exists sch_debezium CASCADE;";
}
