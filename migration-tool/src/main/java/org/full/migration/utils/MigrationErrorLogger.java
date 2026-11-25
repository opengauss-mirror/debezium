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

package org.full.migration.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * MigrationErrorLogger
 * Used to record failed statements and exception information during the migration process
 * @since 2026-04-16
 */
public class MigrationErrorLogger {
    private static final Logger LOGGER = LoggerFactory.getLogger(MigrationErrorLogger.class);
    private static final String ERROR_LOG_DIR = "process";
    private static final String ERROR_LOG_FILE_PREFIX = "migration_error";
    private static final String ERROR_LOG_FILE_SUFFIX = ".log";
    private static volatile MigrationErrorLogger instance;
    private final String errorLogFilePath;

    private MigrationErrorLogger() {
        this.errorLogFilePath = ERROR_LOG_DIR + File.separator + ERROR_LOG_FILE_PREFIX + ERROR_LOG_FILE_SUFFIX;
        File errorLogDir = new File(ERROR_LOG_DIR);
        if (!errorLogDir.exists() && !errorLogDir.mkdirs()) {
            LOGGER.warn("Failed to create error log directory: {}", ERROR_LOG_DIR);
        }
        LOGGER.info("Migration error log file: {}", errorLogFilePath);
    }

    /**
     * get MigrationErrorLogger instance
     *
     * @return MigrationErrorLogger instance
     */
    public static MigrationErrorLogger getInstance() {
        if (instance == null) {
            synchronized (MigrationErrorLogger.class) {
                if (instance == null) {
                    instance = new MigrationErrorLogger();
                }
            }
        }
        return instance;
    }

    /**
     * log create table error information
     *
     * @param schemaName  schema name
     * @param tableName   table name
     * @param createTableSql create table sql
     * @param errorMessage error message
     */
    public void logCreateTableError(String schemaName, String tableName, String createTableSql, String errorMessage) {
        String logEntry = buildCreateTableErrorEntry(schemaName, tableName, createTableSql, errorMessage);
        writeToLogFile(logEntry);
    }

    /**
     * log sql error information
     *
     * @param operation operation type
     * @param objectName object name
     * @param sql SQL statement
     * @param errorMessage error message
     */
    public void logSqlError(String operation, String objectName, String sql, String errorMessage) {
        String logEntry = buildSqlErrorEntry(operation, objectName, sql, errorMessage);
        writeToLogFile(logEntry);
    }

    private String buildCreateTableErrorEntry(String schemaName, String tableName, String createTableSql, String errorMessage) {
        StringBuilder sb = new StringBuilder();
        sb.append("\n==========================================\n");
        sb.append("[ERROR] CREATE TABLE FAILED\n");
        sb.append("Timestamp: ").append(new Date()).append("\n");
        sb.append("Schema: " + schemaName).append("\n");
        sb.append("Table: " + tableName).append("\n");
        sb.append("Error Message: " + errorMessage).append("\n");
        sb.append("Create Table SQL: \n").append(createTableSql).append("\n");
        sb.append("==========================================\n");
        return sb.toString();
    }

    private String buildSqlErrorEntry(String operation, String objectName, String sql, String errorMessage) {
        StringBuilder sb = new StringBuilder();
        sb.append("\n==========================================\n");
        sb.append("[ERROR] SQL EXECUTION FAILED\n");
        sb.append("Timestamp: ").append(new Date()).append("\n");
        sb.append("Operation: " + operation).append("\n");
        sb.append("Object: " + objectName).append("\n");
        sb.append("Error Message: " + errorMessage).append("\n");
        sb.append("SQL: \n").append(sql).append("\n");
        sb.append("==========================================\n");
        return sb.toString();
    }

    private void writeToLogFile(String logEntry) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(errorLogFilePath, true))) {
            writer.write(logEntry);
            writer.flush();
        } catch (IOException e) {
            LOGGER.error("Failed to write to error log file: {}", errorLogFilePath, e);
        }
    }
}