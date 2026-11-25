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

package org.full.migration.test.manager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.full.migration.test.cleanup.EnvironmentCleanupService;
import org.full.migration.test.config.ConfigManager;
import org.full.migration.test.cleanup.DatabaseCleanupService;

import java.io.File;
import java.sql.Connection;

/**
 * TestEnvironmentManager
 * Manages test environment setup and cleanup for oracle2ograc migration testing
 * This class provides functionality to clean up and prepare test environments
 * by executing SQL scripts from specified scenario directories
 */
public class TestEnvironmentManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestEnvironmentManager.class);
    private final EnvironmentCleanupService cleanupService;
    private final ConfigManager configManager;

    /**
     * Constructor
     * @param configFilePath Configuration file path
     */
    public TestEnvironmentManager(String configFilePath) {
        this.configManager = new ConfigManager(configFilePath);
        this.cleanupService = new DatabaseCleanupService(configManager);
    }

    /**
     * Get test scenario path from configuration
     * @return Test scenario path
     */
    private String getTestScenarioPath() {
        return cleanupService.getTestScenarioPath();
    }

    /**
     * Clean up test environment
     * Cleans up all test objects from both source and target databases
     */
    public void cleanup() {
        LOGGER.info("Cleaning up test environment...");
        try {
            cleanupService.cleanup();
            LOGGER.info("Test environment cleanup completed");
        } catch (Exception e) {
            LOGGER.error("Error cleaning up test environment: " + e.getMessage(), e);
        }
    }

    /**
     * Prepare test environment with test data
     * First cleans up the environment, then executes test scripts from the specified scenario
     * @param scenario Test scenario name
     */
    public void prepare(String scenario) {
        LOGGER.info("Preparing test environment with scenario: {}", scenario);
        try {
            cleanup();
            String scenarioPath = getTestScenarioPath() + File.separator + scenario;
            File scenarioDir = new File(scenarioPath);
            if (!scenarioDir.exists() || !scenarioDir.isDirectory()) {
                LOGGER.error("Test scenario directory not found: {}", scenarioPath);
                return;
            }
            Connection sourceConn = cleanupService.getSourceConnection();
            try {
                executeTestScripts(sourceConn, scenarioDir);
                LOGGER.info("Test data prepared successfully");
                gatherSchemaStats(sourceConn, cleanupService.getOwner());
            } finally {
                if (sourceConn != null) {
                    try { sourceConn.close(); } catch (Exception e) { LOGGER.warn("Error closing source connection: " + e.getMessage()); }
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error preparing test environment: " + e.getMessage(), e);
        }
    }

    /**
     * Gather schema statistics using DBMS_STATS
     * @param sourceConn Source database connection
     * @param owner Database owner/schema name
     */
    private void gatherSchemaStats(Connection sourceConn, String owner) {
        LOGGER.info("Gathering schema statistics for owner: {}", owner);
        try (java.sql.Statement stmt = sourceConn.createStatement()) {
            String sql = "BEGIN DBMS_STATS.GATHER_SCHEMA_STATS(OWNNAME => '" + owner + "', CASCADE => TRUE); END;";
            stmt.execute(sql);
            LOGGER.info("Schema statistics gathered successfully");
        } catch (Exception e) {
            LOGGER.error("Error gathering schema statistics: " + e.getMessage(), e);
        }
    }

    /**
     * Execute test scripts in the scenario directory
     * @param conn Database connection
     * @param scenarioDir Scenario directory
     */
    private void executeTestScripts(Connection conn, File scenarioDir) {
        File[] sqlFiles = scenarioDir.listFiles((dir, name) -> name.toLowerCase().endsWith(".sql"));
        if (sqlFiles == null || sqlFiles.length == 0) {
            LOGGER.warn("No SQL files found in scenario directory: {}", scenarioDir.getAbsolutePath());
            return;
        }

        for (File sqlFile : sqlFiles) {
            // Check if this is the general data procedure script
            if (sqlFile.getName().equals("general_data_test_procedure.sql")) {
                // Get the configuration value, default to false
                boolean executeGeneralDataProcedure = getExecuteGeneralDataProcedure();
                if (!executeGeneralDataProcedure) {
                    LOGGER.info("Skipping execution of general_data_test_procedure.sql as configured");
                    continue;
                }
            }
            
            try {
                executeSqlFile(conn, sqlFile);
            } catch (Exception e) {
                LOGGER.warn("Error executing script {}: ", sqlFile.getName(), e);
            }
        }
    }

    /**
     * Get the configuration value for executing general data procedure
     * @return true if the general data procedure should be executed, false otherwise
     */
    private boolean getExecuteGeneralDataProcedure() {
        // Get the scripts section from config
        @SuppressWarnings("unchecked")
        java.util.Map<String, Object> testConfig = (java.util.Map<String, Object>) configManager.getConfigValue("test", "scripts", new java.util.HashMap<String, Object>());
        if (testConfig != null) {
            Object executeValue = testConfig.get("executeGeneralDataProcedure");
            if (executeValue instanceof Boolean) {
                return (Boolean) executeValue;
            }
        }
        // Default to false
        return false;
    }

    /**
     * Execute SQL file using JDBC Statement
     * Supports both regular SQL statements and PL/SQL blocks
     * @param conn Database connection
     * @param sqlFile SQL file to execute
     * @throws Exception if execution fails
     */
    private void executeSqlFile(Connection conn, File sqlFile) throws Exception {
        LOGGER.info("Executing test script: {}", sqlFile.getName());

        java.io.BufferedReader reader = new java.io.BufferedReader(new java.io.FileReader(sqlFile));
        StringBuilder currentStatement = new StringBuilder();
        String line;
        boolean isInPlSqlBlock = false;
        
        java.sql.Statement stmt = conn.createStatement();
        try {
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("--")) {
                    continue;
                }
                
                // Handle SQL*Plus @ command for including other scripts
                if (line.startsWith("@")) {
                    String includePath = line.substring(1).trim();
                    // For combined test script, resolve paths from oracle_test_scenario directory
                    File baseDir = sqlFile.getParentFile();
                    if (baseDir.getName().equals("combined")) {
                        baseDir = baseDir.getParentFile(); // Go up to oracle_test_scenario directory
                    }
                    File includedFile = new File(baseDir, includePath);
                    if (includedFile.exists() && includedFile.isFile()) {
                        if (isLogEnabled()) {
                            System.out.println("Executing included script: " + includePath);
                        }
                        executeSqlFile(conn, includedFile);
                    } else {
                        if (isLogEnabled()) {
                            System.err.println("Included script not found: " + includePath);
                            System.err.println("Expected path: " + includedFile.getAbsolutePath());
                        }
                    }
                    continue;
                }
                
                if (line.equals("/")) {
                    String statement = currentStatement.toString().trim();
                    if (!statement.isEmpty()) {
                        if (isLogEnabled()) {
                            System.out.println(statement);
                        }
                        try {
                            stmt.execute(statement);
                        } catch (Exception e) {
                            if (isLogEnabled()) {
                                System.err.println("Error executing: " + statement);
                                System.err.println("Cause: " + e.getMessage());
                            }
                        }
                        currentStatement = new StringBuilder();
                        isInPlSqlBlock = false;
                    }
                } else if (line.startsWith("CREATE OR REPLACE FUNCTION") || line.startsWith("CREATE OR REPLACE PROCEDURE") || line.startsWith("CREATE OR REPLACE TRIGGER")) {
                    isInPlSqlBlock = true;
                    currentStatement.append(line).append("\n");
                } else if (line.startsWith("DECLARE") || line.startsWith("BEGIN")) {
                    isInPlSqlBlock = true;
                    currentStatement.append(line).append("\n");
                } else if (line.startsWith("END") && isInPlSqlBlock) {
                    currentStatement.append(line).append("\n");
                } else if (line.endsWith(";")) {
                    currentStatement.append(line).append("\n");
                    
                    if (!isInPlSqlBlock) {
                        String statement = currentStatement.toString().trim();
                        statement = statement.replaceAll(";\s*$", "");
                        
                        if (!statement.isEmpty()) {
                            if (isLogEnabled()) {
                                System.out.println(statement);
                            }
                            try {
                                stmt.execute(statement);
                            } catch (Exception e) {
                                if (isLogEnabled()) {
                                    System.err.println("Error executing: " + statement);
                                    System.err.println("Cause: " + e.getMessage());
                                }
                            }
                            currentStatement = new StringBuilder();
                        }
                    }
                } else {
                    currentStatement.append(line).append("\n");
                }
            }
            
            String statement = currentStatement.toString().trim();
            if (!statement.isEmpty()) {
                statement = statement.replaceAll(";\s*$", "");
                
                if (!statement.isEmpty()) {
                    if (isLogEnabled()) {
                        System.out.println(statement);
                    }
                    try {
                        stmt.execute(statement);
                    } catch (Exception e) {
                        if (isLogEnabled()) {
                            System.err.println("Error executing: " + statement);
                            System.err.println("Cause: " + e.getMessage());
                        }
                    }
                }
            }
        } finally {
            reader.close();
            stmt.close();
        }
        LOGGER.info("Script executed successfully: {}", sqlFile.getName());
    }

    /**
     * Check if logging is enabled
     * @return true if logging is enabled
     */
    private boolean isLogEnabled() {
        return "enabled".equals(cleanupService.getTestLog());
    }
}