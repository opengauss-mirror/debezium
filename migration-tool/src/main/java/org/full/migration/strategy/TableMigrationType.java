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

import lombok.Getter;

import org.full.migration.model.TaskTypeEnum;
import org.full.migration.source.SourceDatabase;
import org.full.migration.target.TargetDatabase;

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
    INDEX(TaskTypeEnum.INDEX.getTaskType(), SourceDatabase::readTableIndex, TargetDatabase::writeTableIndex),
    CONSTRAINT(TaskTypeEnum.CONSTRAINT.getTaskType(), SourceDatabase::readConstraints,
        TargetDatabase::writeConstraints),
    PRIMARY_KEY(TaskTypeEnum.PRIMARY_KEY.getTaskType(), SourceDatabase::readTablePk, TargetDatabase::writeTablePk),
    FOREIGN_KEY(TaskTypeEnum.FOREIGN_KEY.getTaskType(), SourceDatabase::readTableFk, TargetDatabase::writeTableFk);

    private final String type;
    private final BiConsumer<SourceDatabase, Set<String>> readTask;
    private final Consumer<TargetDatabase> writeTask;

    /**
     * TableMigrationType
     *
     * @param type type
     * @param readTask readTask
     * @param writeTask writeTask
     */
    TableMigrationType(String type, BiConsumer<SourceDatabase, Set<String>> readTask,
        Consumer<TargetDatabase> writeTask) {
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
