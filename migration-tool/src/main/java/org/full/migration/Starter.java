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

package org.full.migration;

import org.full.migration.constants.CommonConstants;
import org.full.migration.coordinator.MigrationEngine;
import org.full.migration.logger.LogManager;
import org.full.migration.utils.MemoryUtils;

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
        System.setProperty("logback.statusListenerClass", "ch.qos.logback.core.status.NopStatusListener");
        
        if (args.length % 2 != 0) {
            System.err.println("input error, please check parameters.");
            return;
        }
        for (int i = 0; i < args.length; i += 2) {
            commandMap.put(args[i], args[i + 1]);
        }

        String logFile = commandMap.get("--log-file");
        LogManager.initializeLogFile(logFile);
        String logLevel = commandMap.get("--log-level");
        LogManager.setLogLevel(logLevel);
        MemoryUtils.warnIfPhysicalMemoryInsufficient();
        
        String taskType = commandMap.get(CommonConstants.TASK_TYPE);
        String sourceDatabase = commandMap.get(CommonConstants.SOURCE_DATABASE);
        String configPath = commandMap.get(CommonConstants.CONFIG_PATH);
        MigrationEngine dispatcher = new MigrationEngine(taskType, sourceDatabase, configPath);
        dispatcher.dispatch();
    }
}