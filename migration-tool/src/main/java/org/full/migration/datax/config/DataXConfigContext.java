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

import lombok.Data;
import org.full.migration.model.config.DatabaseConfig;
import org.full.migration.model.table.Table;

/**
 * DataXConfigContext
 * DataX configuration generation context
 * Encapsulates all parameters required for strategy generation configuration
 *
 * @since 2025-04-18
 */
@Data
public class DataXConfigContext {
    private DatabaseConfig sourceConfig;
    private DatabaseConfig targetConfig;
    private String schemaName;
    private String tableName;
    private String targetSchemaName;
    private Table table;
    private DataXCommonConfig commonConfig;
    
    /**
     * Constructor (does not set commonConfig parameter)
     */
    public DataXConfigContext(DatabaseConfig sourceConfig, DatabaseConfig targetConfig,
                             String schemaName, String tableName, String targetSchemaName,
                             Table table) {
        this.sourceConfig = sourceConfig;
        this.targetConfig = targetConfig;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.targetSchemaName = targetSchemaName;
        this.table = table;
        this.commonConfig = null;
    }
}
