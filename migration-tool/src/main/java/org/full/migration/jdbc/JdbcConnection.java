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

package org.full.migration.jdbc;

import org.full.migration.model.config.DatabaseConfig;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * JdbcConnection
 *
 * @since 2025-04-18
 */
public interface JdbcConnection {
    /**
     * RETRY_TIME
     */
    int RETRY_TIME = 10;

    /**
     * SLEEP_TIME
     */
    int SLEEP_TIME = 3000;

    /**
     * getConnection
     *
     * @param dbConfig dbConfig
     * @return Connection
     * @throws SQLException SQLException
     */
    Connection getConnection(DatabaseConfig dbConfig) throws SQLException;

    /**
     * retryConnection
     *
     * @param dbConfig dbConfig
     * @return Connection
     * @throws SQLException SQLException
     */
    Connection retryConnection(DatabaseConfig dbConfig) throws SQLException;
}
