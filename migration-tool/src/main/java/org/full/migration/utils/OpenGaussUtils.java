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

package org.full.migration.utils;

import org.full.migration.constants.OpenGaussConstants;
import org.full.migration.enums.SqlCompatibilityEnum;
import org.full.migration.jdbc.OpenGaussConnection;
import org.full.migration.model.config.DatabaseConfig;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;

/**
 * OpenGaussUtils
 *
 * @since 2026-03-13
 */
public class OpenGaussUtils {
    /**
     * Get the SQL compatibility enum from OpenGauss.
     *
     * @param databaseConfig the database config
     * @return the SQL compatibility enum
     * @throws SQLException if an SQL error occurs
     */
    public static SqlCompatibilityEnum getSqlCompatibilityEnum(DatabaseConfig databaseConfig) throws SQLException {
        OpenGaussConnection openGaussConnection = new OpenGaussConnection();
        try (Connection conn = openGaussConnection.getConnection(databaseConfig);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(OpenGaussConstants.SHOW_SQL_COMPATIBILITY)) {
            if (rs.next()) {
                String sqlCompatibility = rs.getString(1);
                return SqlCompatibilityEnum.valueOf(sqlCompatibility.toUpperCase(Locale.ROOT));
            }
            throw new SQLException("Show sql_compatibility result is empty");
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Unsupported openGauss sql_compatibility", e);
        }
    }

    /**
     * Open dolphin.sql_mode ansi_quotes.
     *
     * @param connection the openGauss connection
     * @throws SQLException if a database access error occurs
     */
    public static void openDolphinSqlModeAnsiQuotes(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(OpenGaussConstants.OPENGAUSS_SHOW_DOLPHIN_SQL_MODE)) {
            if (rs.next()) {
                String currentValue = rs.getString("dolphin.sql_mode");
                if (!currentValue.contains("ansi_quotes")) {
                    statement.executeUpdate(String.format(OpenGaussConstants.OPENGAUSS_SET_DOLPHIN_SQL_MODE_MODEL,
                            currentValue + ",ansi_quotes"));
                }
            }
        }
    }
}
