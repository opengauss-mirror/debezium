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

package org.full.migration;

import org.full.migration.constants.CommonConstants;
import org.full.migration.coordinator.MigrationEngine;

import java.util.HashMap;
import java.util.Map;

/**
 * Starter
 *
 * @since 2025-04-18
 */
public class Starter {
    private static Map<String, String> commandMap = new HashMap<>();

    /**
     * main
     *
     * @param args args
     */
    public static void main(String[] args) {
        for (int i = 0; i < args.length; i += 2) {
            commandMap.put(args[i], args[i + 1]);
        }
        String taskType = commandMap.get(CommonConstants.TASK_TYPE);
        String sourceDatabase = commandMap.get(CommonConstants.SOURCE_DATABASE);
        String configPath = commandMap.get(CommonConstants.CONFIG_PATH);
        MigrationEngine dispatcher = new MigrationEngine(taskType, sourceDatabase, configPath);
        dispatcher.dispatch();
    }
}