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

package org.full.migration.coordinator.check;

import org.full.migration.model.config.GlobalConfig;

/**
 * Parameter check strategy for the default scenario
 * (applicable to migrations from SQL Server, PostgreSQL, etc., to openGauss)
 * 
 */
public class DefaultScenarioCheckStrategy implements ScenarioParamCheckStrategy {
    @Override
    public boolean check(GlobalConfig globalConfig, String taskType) {
        if ("table".equalsIgnoreCase(taskType)) {
            // Default scenario table migration check
            return globalConfig.getSourceConfig().isValid(taskType);
        }
        return true;
    }
    
    @Override
    public String getSupportedSourceType() {
        return "default";
    }
}