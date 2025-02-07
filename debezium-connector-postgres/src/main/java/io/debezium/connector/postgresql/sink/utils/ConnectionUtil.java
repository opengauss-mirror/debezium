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

package io.debezium.connector.postgresql.sink.utils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import io.debezium.connector.postgresql.sink.object.ConnectionInfo;
import io.debezium.connector.postgresql.migration.PostgresSqlConstant;
import org.postgresql.core.ServerVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ConnectionUtil
 *
 * @author tianbin
 * @since 2024/11/27
 */
public class ConnectionUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionUtil.class);

    /**
     * Create connection by name
     *
     * @param connectionInfo ConnectionInfo
     * @return connection
     */
    public static Connection createConnection(ConnectionInfo connectionInfo) {
        return connectionInfo.createOpenGaussConnection();
    }

    /**
     * Get server version
     *
     * @param conn Connection
     * @return server version
     */
    public static String getServerVersion(Connection conn) {
        String pgServerVersion = "";
        try (Statement stmt = conn.createStatement();
             ResultSet rst = stmt.executeQuery(PostgresSqlConstant.PG_SHOW_SERVER_VERSION)) {
            if (rst.next()) {
                pgServerVersion = rst.getString(1);
            }
        } catch (SQLException e) {
            LOGGER.error("Get server version failed.", e);
        }
        return pgServerVersion;
    }

    /**
     * Get server version num
     *
     * @param versionStr version string
     * @return server version num
     */
    public static int getServerVersionNum(String versionStr) {
        return ServerVersion.from(versionStr).getVersionNum();
    }

    /**
     * Is server version less than compared version
     *
     * @param conn Connection
     * @param comparedVersion compared version
     * @return is server version less than compared version
     */
    public static boolean isServerVersionLessThan(Connection conn, String comparedVersion) {
        return getServerVersionNum(getServerVersion(conn)) < getServerVersionNum(comparedVersion);
    }
}
