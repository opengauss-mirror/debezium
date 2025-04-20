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
