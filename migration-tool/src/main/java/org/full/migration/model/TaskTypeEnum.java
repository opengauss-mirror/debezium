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
    PRIMARY_KEY("primarykey"),
    FOREIGN_KEY("foreignkey"),
    INDEX("index"),
    VIEW("view"),
    FUNCTION("function"),
    TRIGGER("trigger"),
    PROCEDURE("procedure"),
    SEQUENCE("sequence"),
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
