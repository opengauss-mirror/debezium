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

package org.full.migration.strategy;

import org.full.migration.source.SourceDatabase;
import org.full.migration.target.TargetDatabase;

import java.util.HashMap;
import java.util.Map;

/**
 * StrategyFactory
 *
 * @since 2025-03-15
 */
public class StrategyFactory {
    private static final Map<String, MigrationStrategy> strategyMap = new HashMap<>();

    /**
     * buildStrategyMap
     *
     * @param sourceDatabase sourceDatabase
     * @param targetDatabase targetDatabase
     */
    public static void buildStrategyMap(SourceDatabase sourceDatabase, TargetDatabase targetDatabase) {
        strategyMap.put("table", new TableMigration(sourceDatabase, targetDatabase));
        strategyMap.put("index", new IndexMigration(sourceDatabase, targetDatabase));
        strategyMap.put("view", new ObjectMigration(sourceDatabase, targetDatabase, "view"));
        strategyMap.put("function", new ObjectMigration(sourceDatabase, targetDatabase, "function"));
        strategyMap.put("trigger", new ObjectMigration(sourceDatabase, targetDatabase, "trigger"));
        strategyMap.put("procedure", new ObjectMigration(sourceDatabase, targetDatabase, "procedure"));
    }

    /**
     * getMigrationStrategy
     *
     * @param migrationType migrationType
     * @return MigrationStrategy
     */
    public static MigrationStrategy getMigrationStrategy(String migrationType) {
        return strategyMap.get(migrationType);
    }
}
