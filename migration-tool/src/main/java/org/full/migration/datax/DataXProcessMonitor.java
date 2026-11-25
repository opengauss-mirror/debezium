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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * DataXProcessMonitor
 * Monitors the parent process and terminates DataX process when parent process exits
 * This class runs in the DataX process itself
 *
 * @since 2026-04-11
 */
public class DataXProcessMonitor {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataXProcessMonitor.class);
    private static final long PARENT_CHECK_INTERVAL = 3; // seconds
    private static final ScheduledExecutorService MONITOR_EXECUTOR = Executors.newSingleThreadScheduledExecutor();
    
    public static void main(String[] args) throws Exception {
        // Start parent process monitor
        startParentProcessMonitor();
        
        try {
            // Try to load and run DataX Engine using reflection
            Class<?> engineClass = Class.forName("com.alibaba.datax.core.Engine");
            java.lang.reflect.Method mainMethod = engineClass.getMethod("main", String[].class);
            mainMethod.invoke(null, (Object) args);
        } catch (ClassNotFoundException e) {
            LOGGER.error("DataX Engine class not found. Make sure DataX is properly installed.", e);
            System.exit(1);
        } catch (Exception e) {
            LOGGER.error("Error running DataX Engine", e);
            System.exit(1);
        }
    }
    
    /**
     * Start parent process monitor
     */
    private static void startParentProcessMonitor() {
        final long parentPid = ProcessHandle.current().parent().map(ProcessHandle::pid).orElse(-1L);
        
        if (parentPid == -1L) {
            LOGGER.warn("Could not determine parent process ID, parent process monitor will not be started");
            return;
        }
        
        LOGGER.info("Starting parent process monitor, parent PID: {}", parentPid);
        
        MONITOR_EXECUTOR.scheduleAtFixedRate(() -> {
            try {
                if (!ProcessHandle.of(parentPid).isPresent()) {
                    // Parent process has exited, terminate DataX process
                    LOGGER.warn("Parent process (PID: {}) has exited, terminating DataX process", parentPid);
                    
                    // Shutdown monitor executor
                    MONITOR_EXECUTOR.shutdown();
                    try {
                        if (!MONITOR_EXECUTOR.awaitTermination(2, TimeUnit.SECONDS)) {
                            MONITOR_EXECUTOR.shutdownNow();
                        }
                    } catch (InterruptedException e) {
                        LOGGER.error("Error shutting down monitor executor", e);
                        Thread.currentThread().interrupt();
                    }
                    
                    // Terminate DataX process
                    System.exit(143); // Exit code indicating process was killed
                }
            } catch (Exception e) {
                LOGGER.warn("Error checking parent process status", e);
            }
        }, PARENT_CHECK_INTERVAL, PARENT_CHECK_INTERVAL, TimeUnit.SECONDS);
        
        // Register shutdown hook to clean up executor
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("Shutdown hook triggered, cleaning up monitor executor");
            MONITOR_EXECUTOR.shutdown();
            try {
                if (!MONITOR_EXECUTOR.awaitTermination(5, TimeUnit.SECONDS)) {
                    MONITOR_EXECUTOR.shutdownNow();
                }
            } catch (InterruptedException e) {
                LOGGER.error("Error shutting down monitor executor", e);
                Thread.currentThread().interrupt();
            }
        }));
    }
}
