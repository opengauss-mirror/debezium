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

package org.full.migration.datax;

import org.full.migration.exception.DataXMigrationException;
import org.full.migration.exception.ErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * DataXInstall
 * DataX installation utility class for cross-platform initialization of DataX
 * tools
 *
 * @since 2025-04-18
 */
public class DataXInstall {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataXInstall.class);
    private static final List<Process> RUNNING_PROCESSES = new CopyOnWriteArrayList<>();
    
    static {
        // Register shutdown hook early to clean up any DataX processes
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("Shutdown hook triggered, cleaning up DataX processes");
            // Clean up processes tracked by DataXInstall
            for (Process process : RUNNING_PROCESSES) {
                if (process.isAlive()) {
                    try {
                        LOGGER.info("Terminating DataX process with PID: {}", process.pid());
                        process.destroy();
                        // Wait a short time for process to exit gracefully
                        if (!process.waitFor(5, java.util.concurrent.TimeUnit.SECONDS)) {
                            LOGGER.warn("DataX process did not exit gracefully, force killing it");
                            process.destroyForcibly();
                        }
                    } catch (InterruptedException e) {
                        LOGGER.error("Error while terminating DataX process", e);
                        Thread.currentThread().interrupt();
                    }
                }
            }
            RUNNING_PROCESSES.clear();
        }));
    }
    

    
    /**
     * Add a process to the tracking list
     * @param process DataX process to track
     */
    public static void addProcess(Process process) {
        RUNNING_PROCESSES.add(process);
        LOGGER.debug("Added DataX process to tracking list, PID: {}", process.pid());
    }
    
    /**
     * Remove a process from the tracking list
     * @param process DataX process to remove
     */
    public static void removeProcess(Process process) {
        if (RUNNING_PROCESSES.remove(process)) {
            LOGGER.debug("Removed DataX process from tracking list, PID: {}", process.pid());
        }
    }
    


    /**
     * Initialize DataX tools
     * Set DataX_HOME environment variable and copy jar files from install_libs
     * directory to corresponding locations
     *
     * @param dataxHome DataX installation directory
     * @throws DataXMigrationException if initialization fails
     */
    public static void initializeDataXTools(String dataxHome) throws DataXMigrationException {
        System.setProperty("DataX_HOME", dataxHome);
        File dataxDir = new File(dataxHome);
        File installLibsDir = new File(dataxDir, "install_libs");
        File propertiesFile = new File(dataxDir, "datax_lib_depends.properties");
        validateDirectoriesAndFiles(installLibsDir, propertiesFile);
        JarRestoreResult result = restoreJarFiles(dataxDir, installLibsDir, propertiesFile);
        displayRestoreResults(result);
        int actualJars = countJars(dataxDir, installLibsDir);
        LOGGER.info("Actually restored jar files: {}  DataX installation completed", actualJars);
    }

    /**
     * Validate necessary directories and files
     *
     * @param installLibsDir install_libs directory
     * @param propertiesFile datax_lib_depends.properties file
     * @throws DataXMigrationException if any directory or file does not exist
     */
    private static void validateDirectoriesAndFiles(File installLibsDir, File propertiesFile)
            throws DataXMigrationException {
        if (!installLibsDir.exists()) {
            throw new DataXMigrationException(ErrorCode.DATAX_TOOLS_INITIALIZATION_ERROR.getCode(),
                    "install_libs directory does not exist");
        }
        if (!propertiesFile.exists()) {
            throw new DataXMigrationException(ErrorCode.DATAX_TOOLS_INITIALIZATION_ERROR.getCode(),
                    "datax_lib_depends.properties file does not exist");
        }
    }

    /**
     * Restore jar files from install_libs directory
     *
     * @param dataxDir       DataX installation directory
     * @param installLibsDir install_libs directory
     * @param propertiesFile datax_lib_depends.properties file
     * @return restore result
     * @throws IOException if reading properties file fails
     */
    private static JarRestoreResult restoreJarFiles(File dataxDir, File installLibsDir, File propertiesFile)
            throws DataXMigrationException {
        int totalConfigs = 0;
        int successfulRestores = 0;
        int failedRestores = 0;
        LOGGER.info("Starting to restore jar files...");
        try (BufferedReader reader = new BufferedReader(new FileReader(propertiesFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) {
                    continue;
                }
                if (line.startsWith("#")) {
                    continue;
                }
                if (line.contains(".jars=")) {
                    totalConfigs++;
                    String[] parts = line.split("\\.jars=", 2);
                    if (parts.length == 2) {
                        if (restoreSingleJar(dataxDir, installLibsDir, parts[0], parts[1])) {
                            successfulRestores++;
                        } else {
                            failedRestores++;
                        }
                    }
                }
            }
        } catch (IOException e) {
            throw new DataXMigrationException(ErrorCode.DATAX_TOOLS_INITIALIZATION_ERROR.getCode(),
                    "Error reading properties file", e);
        }
        return new JarRestoreResult(totalConfigs, successfulRestores, failedRestores);
    }

    /**
     * Restore a single jar file
     *
     * @param dataxDir       DataX installation directory
     * @param installLibsDir install_libs directory
     * @param dirPath        target directory path
     * @param jarName        jar file name
     * @return true if restore successful, false otherwise
     */
    private static boolean restoreSingleJar(File dataxDir, File installLibsDir, String dirPath, String jarName)
            throws DataXMigrationException {
        File fullDirPath = new File(dataxDir, dirPath);
        if (!fullDirPath.exists()) {
            LOGGER.info("Creating directory: {}", fullDirPath.getAbsolutePath());
            if (!fullDirPath.mkdirs()) {
                throw new DataXMigrationException(ErrorCode.DATAX_TOOLS_INITIALIZATION_ERROR.getCode(),
                        "Error creating directory: " + fullDirPath.getAbsolutePath());
            }
        }
        File sourceJar = new File(installLibsDir, jarName);
        if (sourceJar.exists()) {
            File targetJar = new File(fullDirPath, jarName);
            try {
                Files.copy(sourceJar.toPath(), targetJar.toPath(), StandardCopyOption.REPLACE_EXISTING);
                return true;
            } catch (IOException e) {
                throw new DataXMigrationException(ErrorCode.DATAX_TOOLS_INITIALIZATION_ERROR.getCode(),
                        "Error copying jar file", e);
            }
        } else {
            throw new DataXMigrationException(ErrorCode.DATAX_TOOLS_INITIALIZATION_ERROR.getCode(),
                    "Jar file does not exist in install_libs directory: " + jarName);
        }
    }

    /**
     * Display restore results
     *
     * @param result restore result
     */
    private static void displayRestoreResults(JarRestoreResult result) {
        LOGGER.info("====================================");
        LOGGER.info("Total configured jar files: {}", result.getTotalConfigs());
        LOGGER.info("Successfully restored: {}", result.getSuccessfulRestores());
        LOGGER.info("Failed to restore: {}", result.getFailedRestores());
        LOGGER.info("====================================");
    }

    /**
     * Jar restore result
     */
    @Getter
    @AllArgsConstructor
    private static class JarRestoreResult {
        private final int totalConfigs;
        private final int successfulRestores;
        private final int failedRestores;
    }

    /**
     * Count jar files in directory (excluding specified directory)
     *
     * @param directory  directory to count
     * @param excludeDir directory to exclude
     * @return number of jar files
     */
    private static int countJars(File directory, File excludeDir) {
        int count = 0;
        File[] files = directory.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.equals(excludeDir)) {
                    continue;
                }
                if (file.isDirectory()) {
                    count += countJars(file, excludeDir);
                } else if (file.getName().endsWith(".jar")) {
                    count++;
                }
            }
        }
        return count;
    }
}