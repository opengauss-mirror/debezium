/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
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
            stmt.execute(String.format(Locale.ROOT, PostgresSqlConstant.SWITCHSCHEMA, schema));
        } catch (SQLException e) {
            LOGGER.error("postgresql set search_path occurred SQLException", e);
        }
    }
}
