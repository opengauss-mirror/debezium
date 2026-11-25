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

import java.util.ArrayList;
import java.util.List;

/**
 * ReaderParameter
 * Represents the parameter section in DataX configuration for reader.
 * Contains connection information, batch size, query timeout, and split key settings.
 *
 * @since 2025-04-18
 */
@Data
public class ReaderParameter {
    private String username;
    private String password;
    private List<Connection> connection;
    private List<String> column;
    private Integer batchSize = 1024;
    private Integer queryTimeout = 60;
    private String splitPk;

    /**
     * Default constructor that initializes with default values.
     * Default batchSize is 1024, default queryTimeout is 60 seconds.
     */
    public ReaderParameter() {
    }

    /**
     * Add a database connection to the list.
     *
     * @param conn The connection to add
     */
    public void addConnection(Connection conn) {
        if (this.connection == null) {
            this.connection = new ArrayList<>();
        }
        this.connection.add(conn);
    }

    /**
     * Add a column to the list.
     *
     * @param columnName The column name to add
     */
    public void addColumn(String columnName) {
        if (this.column == null) {
            this.column = new ArrayList<>();
        }
        this.column.add(columnName);
    }

    @Override
    public String toString() {
        return "ReaderParameter{connection=" + connection + ", column=" + column + ", batchSize=" + batchSize + ", queryTimeout=" +
                queryTimeout + ", splitPk='" + splitPk + '\'' + '}';
    }
}