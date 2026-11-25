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

package org.full.migration.datax.model;

import java.util.ArrayList;
import java.util.List;

import lombok.Data;

/**
 * Connection
 * Represents the connection part in DataX configuration.
 * Holds the database connection information including JDBC URL, table names,
 * query SQL, credentials, and schema.
 *
 * @since 2025-04-18
 */
@Data
public class Connection {
    private List<String> table;
    private List<String> querySql;
    private String schema;
    
    /**
     * Default constructor that initializes null for jdbcUrl, table, and querySql.
     */
    public Connection() {
        this.table = null;
        this.querySql = null;
    }
    
    /**
     * Constructor with all connection parameters.
     *
     * @param table List of table names
     * @param querySql List of query SQL statements
     * @param schema Database schema name
     */
    public Connection(List<String> table, List<String> querySql, String schema) {
        this.table = table;
        this.querySql = querySql;
        this.schema = schema;
    }

    /**
     * Set the list of table names.
     *
     * @param table The list of table names to set
     */
    public void setTable(List<String> table) {
        if (table == null) {
            table = new ArrayList<>();
        }
        this.table.addAll(table);
    }
    
    /**
     * Add a table name to the list.
     *
     * @param tableName The table name to add
     */
    public void addTable(String tableName) {
        if (this.table == null) {
            this.table = new ArrayList<>();
        }
        this.table.add(tableName);
    }

    /**
     * Add a query SQL statement to the list.
     *
     * @param sql The query SQL statement to add
     */
    public void addQuerySql(String sql) {
        if (this.querySql == null) {
            this.querySql = new ArrayList<>();
        }
        this.querySql.add(sql);
    }
}