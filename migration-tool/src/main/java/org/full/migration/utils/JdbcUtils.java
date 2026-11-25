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

import org.full.migration.constants.OracleSqlConstants;
import org.full.migration.model.config.DatabaseConfig;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * JdbcUtils
 *
 * @since 2026-01-01
 */
public class JdbcUtils {
    private static final Map<String, String> DEFAULT_ORACLE_PARAMS = Map.of(
            "oracle.jdbc.javaTimeSupport", "true",
            "oracle.jdbc.useFetchSizeWithLongColumn", "false",
            "socketTimeout", "90000"
    );
    private static final Map<String, String> DEFAULT_OGRAC_PARAMS = Map.of(
            "socketTimeout", "90000"
    );
    private static final String OGRAC_JDBC_URL = "jdbc:oGRAC://%s:%d?loginTimeout=0";

    /**
     * generateOracleJdbcUrl
     * Generate corresponding jdbcUrl based on databaseConfig
     *
     * @param databaseConfig DatabaseConfig object
     * @return jdbcUrl
     */
    public static String generateOracleJdbcUrl(DatabaseConfig databaseConfig) {
        String jdbcUrlTemplate = String.format(Locale.ROOT, OracleSqlConstants.ORACLE_JDBC_URL,
                databaseConfig.getHost(), databaseConfig.getPort(), databaseConfig.getDatabase());
        return generateJdbcUrl(databaseConfig, jdbcUrlTemplate, DEFAULT_ORACLE_PARAMS);
    }

    /**
     * generateOgracJdbcUrl
     * Generate corresponding jdbcUrl based on databaseConfig
     *
     * @param databaseConfig DatabaseConfig object
     * @return jdbcUrl
     */
    public static String generateOgracJdbcUrl(DatabaseConfig databaseConfig) {
        String jdbcUrlTemplate = String.format(Locale.ROOT, OGRAC_JDBC_URL,
                databaseConfig.getHost(), databaseConfig.getPort());
        return generateJdbcUrl(databaseConfig, jdbcUrlTemplate, DEFAULT_OGRAC_PARAMS);
    }

    private static String generateJdbcUrl(DatabaseConfig databaseConfig, String jdbcUrlTemplate,
                                          Map<String, String> defaultParams) {
        StringBuilder urlBuilder = new StringBuilder(jdbcUrlTemplate);
        Map<String, String> params = new HashMap<>(defaultParams);
        if (databaseConfig.getParams() != null && !databaseConfig.getParams().isEmpty()) {
            params.putAll(databaseConfig.getParams());
        }
        params.forEach((key, value) -> {
            urlBuilder.append("&").append(key).append("=").append(value);
        });
        return urlBuilder.toString();
    }
}
