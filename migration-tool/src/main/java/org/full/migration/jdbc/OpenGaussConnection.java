/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.full.migration.jdbc;

import org.full.migration.object.DatabaseConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * OpenGaussConnection
 *
 * @since 2025-03-15
 */
public class OpenGaussConnection implements JdbcConnection {
    private static final Logger LOGGER = LoggerFactory.getLogger(OpenGaussConnection.class);

    @Override
    public Connection getConnection(DatabaseConfig dbConfig) {
        String sourceUrl = "jdbc:postgresql://" + dbConfig.getHost() + ":" + dbConfig.getPort() + "/"
            + dbConfig.getDatabase() + "?currentSchema=" + dbConfig.getSchema() + "&loggerLevel=error";
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(sourceUrl, dbConfig.getUser(), dbConfig.getPassword());
        } catch (SQLException e) {
            LOGGER.error("Unable to connect to database {}:{}, error message is: {}", dbConfig.getHost(),
                dbConfig.getPort(), e.getMessage());
            System.exit(-1);
        }
        return connection;
    }
}
