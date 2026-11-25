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

package org.full.migration.strategy;

import org.full.migration.model.TaskTypeEnum;
import org.full.migration.source.SourceDatabase;
import org.full.migration.target.ITargetDatabase;

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
    public static void buildStrategyMap(SourceDatabase sourceDatabase, ITargetDatabase targetDatabase) {
        strategyMap.put(TaskTypeEnum.TABLE.getTaskType(), new TableMigration(sourceDatabase, targetDatabase));
        strategyMap.put(TaskTypeEnum.DATAX_TABLE.getTaskType(), new DataxTableMigration(sourceDatabase, targetDatabase));
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
