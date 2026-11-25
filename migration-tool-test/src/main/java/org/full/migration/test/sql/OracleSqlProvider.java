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

package org.full.migration.test.sql;

/**
 * Provides Oracle-specific SQL statements for cleanup operations
 * This implementation is designed for Oracle databases and uses Oracle-specific
 * data dictionary views for querying database objects
 */
public class OracleSqlProvider implements DatabaseSqlProvider {
    @SuppressWarnings("unused")
    private String schema="";
    
    @Override
    public void setSchema(String schema) {
        this.schema = schema;
    }

    @Override
    public String getTriggersQuery() {
        return "SELECT trigger_name FROM user_triggers";
    }

    @Override
    public String getProceduresQuery() {
        return "SELECT object_name FROM user_objects WHERE object_type = 'PROCEDURE'";
    }

    @Override
    public String getFunctionsQuery() {
        return "SELECT object_name FROM user_objects WHERE object_type = 'FUNCTION'";
    }

    @Override
    public String getViewsQuery() {
        return "SELECT view_name FROM user_views";
    }

    /**
     * Get SQL query to retrieve all indexes        
     * SYS_IL%：匹配所有系统大对象索引
     * SYS_C%：过滤系统约束索引（主键 / 唯一键自动生成）
     * SYS_LOB%：过滤 LOB 字段系统索引
     * SYS_IOT_TOP%：过滤索引组织表 (IOT) 的系统索引
     * BIN$%：过滤回收站表
     * @return SQL query string
     */
    @Override
    public String getIndexesQuery() {
        return """
            SELECT index_name FROM user_indexes 
                WHERE table_name NOT LIKE 'BIN$%' 
                AND index_name NOT LIKE 'SYS_%' 
                AND index_name NOT LIKE 'DR$%'
        """;
    }

    @Override
    public String getTablesQuery() {
        return """
            SELECT table_name FROM user_tables 
                WHERE table_name NOT LIKE 'BIN$%' 
                AND table_name NOT LIKE 'DR$%' 
        """;
    }

    @Override
    public String getSequencesQuery() {
        return "SELECT sequence_name FROM user_sequences";
    }

    @Override
    public String getDropTriggerSql(String triggerName) {
        return "DROP TRIGGER " + triggerName;
    }

    @Override
    public String getDropProcedureSql(String procedureName) {
        return "DROP PROCEDURE " + procedureName;
    }

    @Override
    public String getDropFunctionSql(String functionName) {
        return "DROP FUNCTION " + functionName;
    }

    @Override
    public String getDropViewSql(String viewName) {
        return "DROP VIEW " + viewName;
    }

    @Override
    public String getDropIndexSql(String indexName) {
        return "DROP INDEX " + indexName;
    }

    @Override
    public String getDropTableSql(String tableName) {
        return "DROP TABLE " + tableName + " CASCADE CONSTRAINTS";
    }

    @Override
    public String getDropSequenceSql(String sequenceName) {
        return "DROP SEQUENCE " + sequenceName;
    }
}
