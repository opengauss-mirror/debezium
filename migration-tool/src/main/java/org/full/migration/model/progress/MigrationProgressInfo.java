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

package org.full.migration.model.progress;

import lombok.Data;

import org.full.migration.model.TaskTypeEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * MigrationProgressInfo
 *
 * @since 2025-04-18
 */
@Data
public class MigrationProgressInfo {
    private static final Logger LOGGER = LoggerFactory.getLogger(MigrationProgressInfo.class);

    private TotalInfo total;
    private List<ProgressInfo> table = new ArrayList<>();
    private List<ProgressInfo> view = new ArrayList<>();
    private List<ProgressInfo> function = new ArrayList<>();
    private List<ProgressInfo> trigger = new ArrayList<>();
    private List<ProgressInfo> procedure = new ArrayList<>();
    private List<ProgressInfo> sequence = new ArrayList<>();
    private List<ProgressInfo> primarykey = new ArrayList<>();
    private List<ProgressInfo> foreignkey = new ArrayList<>();
    private List<ProgressInfo> index = new ArrayList<>();

    /**
     * add Table progress
     *
     * @param progressInfo ProgressInfo
     */
    public void addTable(ProgressInfo progressInfo) {
        if (table == null) {
            table = new ArrayList<>();
        }
        table.add(progressInfo);
    }

    /**
     * add object progress by objType
     *
     * @param progressInfo ProgressInfo
     * @param objType ObjectEnum
     */
    public void addObject(ProgressInfo progressInfo, TaskTypeEnum objType) {
        switch (objType) {
            case FUNCTION:
                function.add(progressInfo);
                break;
            case VIEW:
                view.add(progressInfo);
                break;
            case TRIGGER:
                trigger.add(progressInfo);
                break;
            case PROCEDURE:
                procedure.add(progressInfo);
                break;
            case SEQUENCE:
                sequence.add(progressInfo);
                break;
            default:
                LOGGER.error("unknown object Type");
        }
    }

    /**
     * add keyAndIndex progress
     *
     * @param progressInfo ProgressInfo
     */
    public void addKeyAndIndex(ProgressInfo progressInfo, TaskTypeEnum keyAndIndexType) {
        switch (keyAndIndexType) {
            case PRIMARY_KEY:
                primarykey.add(progressInfo);
                break;
            case FOREIGN_KEY:
                foreignkey.add(progressInfo);
                break;
            case INDEX:
                index.add(progressInfo);
                break;
            default:
                LOGGER.error("unknown object Type");
        }
    }
}
