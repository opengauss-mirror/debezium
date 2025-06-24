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

package org.full.migration.source;

import org.full.migration.model.config.GlobalConfig;

import java.util.HashMap;
import java.util.Map;

/**
 * SourceDatabaseFactory
 *
 * @since 2025-04-18
 */
public class SourceDatabaseFactory {
    private static final Map<String, SourceDatabase> strategyMap = new HashMap<>();

    /**
     * buildStrategyMap
     *
     * @param globalConfig globalConfig
     */
    public static void buildStrategyMap(GlobalConfig globalConfig) {
        strategyMap.put("sqlserver", new SqlServerSource(globalConfig));
        strategyMap.put("postgresql", new PostgresSource(globalConfig));
    }

    /**
     * getSourceDatabase
     *
     * @param dbType dbType
     * @return SourceDatabase
     */
    public static SourceDatabase getSourceDatabase(String dbType) {
        return strategyMap.get(dbType);
    }
}
