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

package org.full.migration.logger;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * LogManager
 * Manages logging configuration including log file initialization and log level setting
 *
 * @since 2025-04-18
 */
public class LogManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(LogManager.class);

    /**
     * Initialize log file
     * Creates log directory if it doesn't exist
     *
     * @param logFile Path to the log file
     */
    public static void initializeLogFile(String logFile) {
        if (logFile == null) {
            return;
        }
        File logDir = new File(logFile).getParentFile();
        if (logDir != null && !logDir.exists()) {
            if (logDir.mkdirs()) {
                LOGGER.info("Created log directory: {}", logDir.getAbsolutePath());
            } else {
                LOGGER.error("Failed to create log directory: {}", logDir.getAbsolutePath());
            }
        }
        System.setProperty("LOG_FILE", logFile);
        LOGGER.info("Log file initialized: {}", logFile);
    }
    
    /**
     * Set log level
     *
     * @param logLevel Log level (e.g., "INFO", "DEBUG", "ERROR")
     */
    public static void setLogLevel(String logLevel) {
        if (logLevel != null) {
            System.setProperty("LOG_LEVEL", logLevel);
            setLogbackLevel(logLevel);
            LOGGER.info("Log level set to: {}", logLevel);
        }
    }

    /**
     * Set logback log level dynamically
     *
     * @param logLevel Log level string
     */
    private static void setLogbackLevel(String logLevel) {
        try {
            LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
            Level level = Level.toLevel(logLevel, Level.INFO);
            loggerContext.getLogger("ROOT").setLevel(level);
            for (ch.qos.logback.classic.Logger logger : loggerContext.getLoggerList()) {
                logger.setLevel(level);
            }
        } catch (Exception e) {
            LOGGER.error("Failed to set logback log level: {}", e.getMessage());
        }
    }
}