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

import lombok.Getter;
import org.full.migration.model.TaskTypeEnum;
import org.full.migration.source.SourceDatabase;
import org.full.migration.target.ITargetDatabase;

import java.util.Arrays;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * TableMigrationType
 *
 * @since 2025-04-18
 */
@Getter
public enum TableMigrationType {
    INDEX(TaskTypeEnum.INDEX.getTaskType(), SourceDatabase::readTableIndex, ITargetDatabase::writeTableIndex),
    CONSTRAINT(TaskTypeEnum.CONSTRAINT.getTaskType(), SourceDatabase::readConstraints,
            ITargetDatabase::writeConstraints),
    PRIMARY_KEY(TaskTypeEnum.PRIMARY_KEY.getTaskType(), SourceDatabase::readTablePk, ITargetDatabase::writeTablePk),
    FOREIGN_KEY(TaskTypeEnum.FOREIGN_KEY.getTaskType(), SourceDatabase::readTableFk, ITargetDatabase::writeTableFk);

    private final String type;
    private final BiConsumer<SourceDatabase, Set<String>> readTask;
    private final Consumer<ITargetDatabase> writeTask;

    /**
     * TableMigrationType
     *
     * @param type type
     * @param readTask readTask
     * @param writeTask writeTask
     */
    TableMigrationType(String type, BiConsumer<SourceDatabase, Set<String>> readTask,
        Consumer<ITargetDatabase> writeTask) {
        this.type = type;
        this.readTask = readTask;
        this.writeTask = writeTask;
    }

    /**
     * fromType
     *
     * @param type type
     * @return TableMigrationType
     */
    public static TableMigrationType fromType(String type) {
        return Arrays.stream(values())
            .filter(t -> t.type.equalsIgnoreCase(type))
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("Unsupported type: " + type));
    }
}
