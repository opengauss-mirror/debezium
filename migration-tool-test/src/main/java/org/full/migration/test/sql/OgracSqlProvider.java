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
 * Provides OpenGauss (oGRAC)-specific SQL statements for cleanup operations
 * This implementation is designed for OpenGauss databases and uses OpenGauss-specific
 * system catalog views for querying database objects
 */
public class OgracSqlProvider implements DatabaseSqlProvider {
    @SuppressWarnings("unused")
    private String schema="";
    
    @Override
    public void setSchema(String schema) {
        this.schema = schema;
    }

    @Override
    public String getTriggersQuery() {
        return "SELECT TRIGGER_NAME FROM adm_triggers WHERE owner = '" + schema + "'";
    }

    @Override
    public String getProceduresQuery() {
        return "SELECT OBJECT_NAME FROM adm_procedures WHERE owner = '" + schema + "'";
    }

    @Override
    public String getFunctionsQuery() {
        return "SELECT OBJECT_NAME FROM adm_objects WHERE object_type = 'FUNCTION' and owner = '" + schema + "'";
    }

    @Override
    public String getViewsQuery() {
        return "SELECT view_name FROM adm_views WHERE owner = '" + schema + "'";
    }

    @Override
    public String getIndexesQuery() {
        return "SELECT index_name FROM db_indexes WHERE owner = '" + schema + "'";
    }

    @Override
    public String getTablesQuery() {
        return "SELECT table_name FROM adm_tables WHERE owner = '" + schema + "'";
    }

    @Override
    public String getSequencesQuery() {
        return "SELECT sequence_name FROM adm_sequences WHERE SEQUENCE_OWNER = '" + schema + "'";
    }

    @Override
    public String getDropTriggerSql(String triggerName) {
        return "DROP TRIGGER IF EXISTS " + triggerName;
    }

    @Override
    public String getDropProcedureSql(String procedureName) {
        return "DROP PROCEDURE IF EXISTS " + procedureName;
    }

    @Override
    public String getDropFunctionSql(String functionName) {
        return "DROP FUNCTION IF EXISTS " + functionName;
    }

    @Override
    public String getDropViewSql(String viewName) {
        return "DROP VIEW IF EXISTS " + viewName;
    }

    @Override
    public String getDropIndexSql(String indexName) {
        return "DROP INDEX IF EXISTS " + indexName;
    }

    @Override
    public String getDropTableSql(String tableName) {
        return "DROP TABLE IF EXISTS " + tableName + " CASCADE CONSTRAINTS PURGE";
    }

    @Override
    public String getDropSequenceSql(String sequenceName) {
        return "DROP SEQUENCE IF EXISTS " + sequenceName;
    }
}
