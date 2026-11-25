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

import lombok.Data;

import java.util.List;

/**
 * Connection
 * Represents the connection part in DataX configuration.
 * Holds the database connection information including JDBC URL, table names,
 * query SQL, credentials, and schema.
 *
 * @since 2025-04-18
 */
@Data
public class WriterConnection extends Connection{
    private String jdbcUrl;

    /**
     * Default constructor that initializes null for jdbcUrl, table, and querySql.
     */
    public WriterConnection() {
        super();
        this.jdbcUrl = null;
    }

    @Override
    public String toString() {
        return "WriterConnection{jdbcUrl=" + jdbcUrl +", table=" + getTable() +
                ", querySql=" + getQuerySql() +", schema='" + getSchema() + '\'' +'}';
    }
}