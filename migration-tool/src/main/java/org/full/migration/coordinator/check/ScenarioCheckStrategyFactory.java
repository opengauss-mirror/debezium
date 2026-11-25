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

import java.util.HashMap;
import java.util.Map;

/**
 * Factory class for creating scenario parameter check strategies
 */
public class ScenarioCheckStrategyFactory {
    private static final Map<String, ScenarioParamCheckStrategy> strategyMap = new HashMap<>();
    
    static {
        // Register all supported check strategies
        registerStrategy(new DefaultScenarioCheckStrategy());
        registerStrategy(new OracleToOgracCheckStrategy());
    }
    
    /**
     * Register a check strategy for a specific source type
     * @param strategy The check strategy instance
     */
    private static void registerStrategy(ScenarioParamCheckStrategy strategy) {
        strategyMap.put(strategy.getSupportedSourceType(), strategy);
    }
    
    /**
     * Get the check strategy for a specific source type
     * @param sourceType The source database type
     * @return The check strategy instance
     */
    public static ScenarioParamCheckStrategy getStrategy(String sourceType) {
        if (sourceType == null) {
            return strategyMap.get("default");
        }
        
        ScenarioParamCheckStrategy strategy = strategyMap.get(sourceType.toLowerCase());
        return strategy != null ? strategy : strategyMap.get("default");
    }
}