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

package io.debezium.connector.postgresql.migration;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Description: migration utils
 *
 * @author jianghongbo
 * @since 2024/11/30
 */
public class MigrationUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(MigrationUtil.class);

    /**
     * log report slice interval
     */
    public static final Integer LOG_REPORT_INTERVAL_SLICES = 50;

    /**
     * switch postgresql schema
     *
     * @param schema String
     * @param connection Connection
     */
    public static void switchSchema(String schema, Connection connection) {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(String.format(Locale.ROOT, PostgresSqlConstant.SWITCHSCHEMA, quotePgIdent(schema)));
        } catch (SQLException e) {
            LOGGER.error("postgresql set search_path occurred SQLException", e);
        }
    }

    private static String quotePgIdent(String identifier) {
        return "\"" + (identifier == null ? "" : identifier.replace("\"", "\"\"")) + "\"";
    }
}
