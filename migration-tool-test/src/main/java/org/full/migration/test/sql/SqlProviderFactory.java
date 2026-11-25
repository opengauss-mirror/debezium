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

package org.full.migration.test.sql;

/**
 * SqlProviderFactory
 * Factory class for creating database-specific SQL providers
 * This factory creates appropriate SQL provider instances based on the database type
 */
public class SqlProviderFactory {
    /**
     * Get a SQL provider instance based on the database type
     * @param dbType Database type (oracle or ograc)
     * @return DatabaseSqlProvider instance for the specified database type
     * @throws IllegalArgumentException if the database type is not supported
     */
    public static DatabaseSqlProvider getSqlProvider(String dbType) {
        if ("oracle".equals(dbType)) {
            return new OracleSqlProvider();
        } else if ("ograc".equals(dbType)) {
            return new OgracSqlProvider();
        }
        throw new IllegalArgumentException("Unsupported database type: " + dbType);
    }
}