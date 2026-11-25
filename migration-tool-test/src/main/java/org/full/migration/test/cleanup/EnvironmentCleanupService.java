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

package org.full.migration.test.cleanup;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * EnvironmentCleanupService
 * Service interface for environment cleanup operations
 * This interface defines the contract for managing database connections
 * and cleaning up test environments in oracle2ograc migration scenarios
 */
public interface EnvironmentCleanupService {
    /**
     * Get source database connection
     * @return Source database connection
     * @throws SQLException if database connection fails
     */
    Connection getSourceConnection() throws SQLException;
    
    /**
     * Get target database connection
     * @return Target database connection
     * @throws SQLException if database connection fails
     */
    Connection getTargetConnection() throws SQLException;
    
    /**
     * Get test scenario path from configuration
     * @return Test scenario path
     */
    String getTestScenarioPath();
    
    /**
     * Get test log configuration
     * @return Test log configuration (enabled or disabled)
     */
    String getTestLog();
    
    /**
     * Get database owner from configuration
     * @return Database owner
     */
    String getOwner();
    
    /**
     * Clean up test environment
     * Cleans up all test objects from both source and target databases
     * @throws Exception if cleanup fails
     */
    void cleanup() throws Exception;
}