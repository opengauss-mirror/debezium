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

package org.full.migration.source;

import org.full.migration.object.SourceConfig;

import java.util.HashMap;
import java.util.Map;

/**
 * SourceDatabaseFactory
 *
 * @since 2025-03-15
 */
public class SourceDatabaseFactory {
    private static final Map<String, SourceDatabase> strategyMap = new HashMap<>();

    /**
     * buildStrategyMap
     *
     * @param sourceConfig sourceConfig
     */
    public static void buildStrategyMap(SourceConfig sourceConfig) {
        strategyMap.put("sqlserver", new SqlServerSource(sourceConfig));
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
