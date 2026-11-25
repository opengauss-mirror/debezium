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

package org.full.migration.model;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * ObjectTypeEnum
 *
 * @since 2025-04-18
 */
@Getter
@AllArgsConstructor
public enum TaskTypeEnum {
    TABLE("table"),
    DATAX_TABLE("datax_table"),
    SEQUENCE("sequence"),
    PRIMARY_KEY("primarykey"),
    FOREIGN_KEY("foreignkey"),
    INDEX("index"),
    CONSTRAINT("constraint"),
    VIEW("view"),
    FUNCTION("function"),
    TRIGGER("trigger"),
    PROCEDURE("procedure"),
    DROP_REPLICA_SCHEMA("drop_replica_schema"),
    UNKNOWN("unknown");

    private String taskType;

    /**
     * getTaskTypeEnum
     *
     * @param type type
     * @return TaskTypeEnum
     */
    public static TaskTypeEnum getTaskTypeEnum(String type) {
        for (TaskTypeEnum typeEnum : TaskTypeEnum.values()) {
            if (typeEnum.getTaskType().equalsIgnoreCase(type)) {
                return typeEnum;
            }
        }
        return UNKNOWN;
    }
}
