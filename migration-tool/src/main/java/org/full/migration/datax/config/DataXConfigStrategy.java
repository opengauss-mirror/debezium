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

package org.full.migration.datax.config;

import org.full.migration.datax.model.DataXConfig;
import org.full.migration.exception.DataXMigrationException;

/**
 * DataXConfigStrategy
 * DataX configuration generation strategy interface
 *
 * @since 2025-04-18
 */
public interface DataXConfigStrategy {
    /**
     * Generate DataX configuration object
     *
     * @param context Configuration context
     * @return DataXConfig configuration object
     */
    DataXConfig generateConfig(DataXConfigContext context) throws DataXMigrationException;
    
    /**
     * Get the name of the strategy
     *
     * @return Strategy name
     */
    String getStrategyName();
    
    /**
     * Get the JVM parameters required by this strategy
     * @param rowCount Row count of the table to be migrated
     * @return JVM parameter string
     */
    String getJvmParameters(long rowCount);
}