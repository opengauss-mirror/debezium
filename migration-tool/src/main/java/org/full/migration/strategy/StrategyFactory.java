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

package org.full.migration.strategy;

import org.full.migration.model.TaskTypeEnum;
import org.full.migration.source.SourceDatabase;
import org.full.migration.target.TargetDatabase;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * StrategyFactory
 *
 * @since 2025-04-18
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
        strategyMap.put(TaskTypeEnum.TABLE.getTaskType(), new TableMigration(sourceDatabase, targetDatabase));
        strategyMap.put(TaskTypeEnum.INDEX.getTaskType(),
            new KeyAndIndexMigration(sourceDatabase, targetDatabase, TaskTypeEnum.INDEX.getTaskType()));
        strategyMap.put(TaskTypeEnum.PRIMARY_KEY.getTaskType(),
            new KeyAndIndexMigration(sourceDatabase, targetDatabase, TaskTypeEnum.PRIMARY_KEY.getTaskType()));
        strategyMap.put(TaskTypeEnum.FOREIGN_KEY.getTaskType(),
            new KeyAndIndexMigration(sourceDatabase, targetDatabase, TaskTypeEnum.FOREIGN_KEY.getTaskType()));
        strategyMap.put(TaskTypeEnum.CONSTRAINT.getTaskType(),
            new KeyAndIndexMigration(sourceDatabase, targetDatabase, TaskTypeEnum.CONSTRAINT.getTaskType()));
        strategyMap.put(TaskTypeEnum.VIEW.getTaskType(),
            new ObjectMigration(sourceDatabase, targetDatabase, TaskTypeEnum.VIEW.getTaskType()));
        strategyMap.put(TaskTypeEnum.FUNCTION.getTaskType(),
            new ObjectMigration(sourceDatabase, targetDatabase, TaskTypeEnum.FUNCTION.getTaskType()));
        strategyMap.put(TaskTypeEnum.TRIGGER.getTaskType(),
            new ObjectMigration(sourceDatabase, targetDatabase, TaskTypeEnum.TRIGGER.getTaskType()));
        strategyMap.put(TaskTypeEnum.PROCEDURE.getTaskType(),
            new ObjectMigration(sourceDatabase, targetDatabase, TaskTypeEnum.PROCEDURE.getTaskType()));
        strategyMap.put(TaskTypeEnum.SEQUENCE.getTaskType(),
            new ObjectMigration(sourceDatabase, targetDatabase, TaskTypeEnum.SEQUENCE.getTaskType()));
        strategyMap.put(TaskTypeEnum.DROP_REPLICA_SCHEMA.getTaskType(),
            new DropReplicaSchema(sourceDatabase, targetDatabase));
    }

    /**
     * getMigrationStrategy
     *
     * @param migrationType migrationType
     * @return MigrationStrategy
     */
    public static MigrationStrategy getMigrationStrategy(String migrationType) {
        return strategyMap.get(migrationType.toLowerCase(Locale.ROOT));
    }
}
