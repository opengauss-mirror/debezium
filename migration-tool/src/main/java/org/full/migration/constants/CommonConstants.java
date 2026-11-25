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
