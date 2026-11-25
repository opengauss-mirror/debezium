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
 * Interface for scenario parameter check strategies
 */
public interface ScenarioParamCheckStrategy {
    /**
     * Check if the global configuration is valid for the specified task type
     * @param globalConfig Global configuration
     * @param taskType Task type
     * @return Whether the configuration is valid
     */
    boolean check(GlobalConfig globalConfig, String taskType);
    
    /**
     * Get the source database type supported by this check strategy
     * @return The supported source database type
     */
    String getSupportedSourceType();
}