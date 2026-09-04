/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

package org.full.migration.utils;

import org.full.migration.exception.DataXMigrationException;
import org.full.migration.exception.ErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicLong;

import com.sun.management.OperatingSystemMXBean;

/**
 * MemoryUtils
 * Utility class for memory-related operations
 *
 * <pre>
 * Memory throttling mechanism for DataX subprocesses:
 * 1. Before launching a DataX process, check the OS actual free memory
 *    (via com.sun.management.OperatingSystemMXBean.getFreeMemorySize())
 * 2. A reservation counter (AtomicLong) tracks the total -Xmx of all DataX
 *    tasks that have acquired memory quota but not yet released it
 *    (semaphore-like byte-based throttling)
 * 3. A task may start only when: OS free memory - reserved memory >= required memory
 * 4. After the DataX process finishes, the reserved quota must be released
 *    via {@link #releaseMemory(String, String)}
 * </pre>
 *
 * @since 2026-04-18
 */
public class MemoryUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(MemoryUtils.class);
    private static final long DEFAULT_MEMORY = 1024 * 1024 * 1024; // 1GB
    private static final int MAX_WAIT_TIME = 300;
    private static final int CHECK_INTERVAL = 10;

    /**
     * Memory reserved for the OS itself so that DataX processes never consume
     * all free physical memory (prevents swap/thrashing). Fixed value: scales
     * poorly with a percentage of total memory, so a flat 1GB reserve covers
     * OS kernel, page cache and the tool JVM's own growth.
     */
    private static final long SYSTEM_RESERVED_MEMORY = 1024 * 1024 * 1024L; // 1GB

    /** Minimum physical memory required by the migration task. */
    private static final long MIN_REQUIRED_PHYSICAL_MEMORY = 8L * 1024 * 1024 * 1024; // 8GB

    /**
     * Total memory (bytes) reserved by DataX tasks that have acquired quota
     * but not yet released it. Acts as a byte-based semaphore to limit the
     * number/size of concurrently running DataX processes.
     */
    private static final AtomicLong RESERVED_MEMORY = new AtomicLong(0);

    /**
     * Check if there is enough OS free memory to execute a task and reserve
     * the required memory quota for it.
     *
     * <pre>Implementation logic:
     * 1. Parse the JVM parameters to extract the required memory (-Xmx parameter)
     * 2. Get the OS actual free memory via OperatingSystemMXBean
     * 3. If OS free memory - reserved memory (by other running DataX tasks)
     *    - system reserved memory >= required memory:
     *    atomically reserve the required memory and return (the caller must
     *    release it via releaseMemory once the task finishes)
     * 4. Otherwise wait and retry every CHECK_INTERVAL seconds, up to
     *    MAX_WAIT_TIME seconds. If memory is still insufficient after the
     *    timeout, throw DataXMigrationException (nothing is reserved in this case)
     * </pre>
     *
     * @param jvmParameters JVM parameters for the task
     * @param taskName Task name for logging
     * @throws DataXMigrationException If there is not enough memory within the wait timeout
     */
    public static void checkMemoryAvailability(String jvmParameters, String taskName) throws DataXMigrationException {
        long requiredMemory = parseRequiredMemory(jvmParameters);

        int waitTime = 0;
        while (true) {
            long osFreeMemory = getOsFreeMemoryBytes();
            long reserved = RESERVED_MEMORY.get();
            if (osFreeMemory - reserved - SYSTEM_RESERVED_MEMORY >= requiredMemory
                    && RESERVED_MEMORY.compareAndSet(reserved, reserved + requiredMemory)) {
                LOGGER.info("Memory quota acquired: osFree={} bytes, reservedByDataX={} bytes, "
                                + "systemReserved={} bytes, required={} bytes for task {}",
                        osFreeMemory, RESERVED_MEMORY.get(), SYSTEM_RESERVED_MEMORY, requiredMemory, taskName);
                return;
            }

            if (waitTime >= MAX_WAIT_TIME) {
                throw new DataXMigrationException(ErrorCode.DATAX_EXECUTION_FAILED.getCode(),
                        "Insufficient memory for task " + taskName
                                + ": osFree=" + osFreeMemory + " bytes, reservedByDataX=" + reserved
                                + " bytes, systemReserved=" + SYSTEM_RESERVED_MEMORY
                                + " bytes, required=" + requiredMemory + " bytes, waited " + waitTime + " seconds");
            }

            LOGGER.info("Waiting for memory: osFree={} bytes, reservedByDataX={} bytes, systemReserved={} bytes, "
                            + "required={} bytes for task {}, waited {} seconds",
                    osFreeMemory, reserved, SYSTEM_RESERVED_MEMORY, requiredMemory, taskName, waitTime);
            try {
                Thread.sleep(CHECK_INTERVAL * 1000L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new DataXMigrationException(ErrorCode.DATAX_EXECUTION_FAILED.getCode(),
                        "Memory check interrupted", e);
            }
            waitTime += CHECK_INTERVAL;
        }
    }

    /**
     * Release the memory quota previously acquired by
     * {@link #checkMemoryAvailability(String, String)} once the DataX task
     * finishes (successfully or not). Must be called from a finally block.
     *
     * @param jvmParameters JVM parameters of the finished task
     * @param taskName Task name for logging
     */
    public static void releaseMemory(String jvmParameters, String taskName) {
        long reservedMemory = parseRequiredMemory(jvmParameters);
        long remaining = RESERVED_MEMORY.addAndGet(-reservedMemory);
        if (remaining < 0) {
            LOGGER.warn("Reserved memory counter went negative ({}), resetting to 0 for task {}",
                    remaining, taskName);
            RESERVED_MEMORY.set(0);
        }
        LOGGER.info("Memory quota released: {} bytes for task {}, remaining reserved: {} bytes",
                reservedMemory, taskName, Math.max(remaining, 0));
    }

    /**
     * Log a warning at startup if the OS total physical memory is below the
     * minimum requirement (8GB). Uses
     * com.sun.management.OperatingSystemMXBean.getTotalMemorySize().
     */
    public static void warnIfPhysicalMemoryInsufficient() {
        Object osBean = ManagementFactory.getOperatingSystemMXBean();
        if (!(osBean instanceof OperatingSystemMXBean)) {
            LOGGER.warn("Cannot detect physical memory: OperatingSystemMXBean not available");
            return;
        }
        long totalMemory = ((OperatingSystemMXBean) osBean).getTotalMemorySize();
        if (totalMemory < MIN_REQUIRED_PHYSICAL_MEMORY) {
            LOGGER.warn("Physical memory {}GB is below the minimum requirement of {}GB; "
                            + "migration may be throttled or fail due to insufficient memory",
                    totalMemory / (1024 * 1024 * 1024), MIN_REQUIRED_PHYSICAL_MEMORY / (1024 * 1024 * 1024));
        } else {
            LOGGER.info("Physical memory check passed: {}GB >= {}GB",
                    totalMemory / (1024 * 1024 * 1024), MIN_REQUIRED_PHYSICAL_MEMORY / (1024 * 1024 * 1024));
        }
    }

    /**
     * Get the OS actual free memory in bytes.
     * Uses com.sun.management.OperatingSystemMXBean.getFreeMemorySize(),
     * falls back to JVM max heap memory if the MXBean is unavailable.
     *
     * @return OS free memory in bytes
     */
    private static long getOsFreeMemoryBytes() {
        Object osBean = ManagementFactory.getOperatingSystemMXBean();
        if (osBean instanceof OperatingSystemMXBean) {
            return ((OperatingSystemMXBean) osBean).getFreeMemorySize();
        }
        LOGGER.debug("OperatingSystemMXBean not available, falling back to JVM max memory");
        return Runtime.getRuntime().maxMemory();
    }

    /**
     * Parse JVM parameters to get required memory in bytes
     * @param jvmParameters JVM parameters string
     * @return Required memory in bytes
     */
    private static long parseRequiredMemory(String jvmParameters) {
        long requiredMemory = 0;
        String[] params = jvmParameters.split(" ");

        for (String param : params) {
            if (param.startsWith("-Xmx")) {
                String memoryStr = param.substring(4);
                requiredMemory = parseMemorySize(memoryStr);
                break;
            }
        }

        if (requiredMemory == 0) {
            requiredMemory = DEFAULT_MEMORY;
        }

        return requiredMemory;
    }

    /**
     * Parse memory size string to bytes
     * @param memoryStr Memory size string (e.g., "512m", "2g")
     * @return Memory size in bytes
     */
    private static long parseMemorySize(String memoryStr) {
        memoryStr = memoryStr.toLowerCase();
        long multiplier = 1;

        if (memoryStr.endsWith("k")) {
            multiplier = 1024;
            memoryStr = memoryStr.substring(0, memoryStr.length() - 1);
        } else if (memoryStr.endsWith("m")) {
            multiplier = 1024 * 1024;
            memoryStr = memoryStr.substring(0, memoryStr.length() - 1);
        } else if (memoryStr.endsWith("g")) {
            multiplier = 1024 * 1024 * 1024;
            memoryStr = memoryStr.substring(0, memoryStr.length() - 1);
        }

        try {
            long size = Long.parseLong(memoryStr);
            return size * multiplier;
        } catch (NumberFormatException e) {
            LOGGER.warn("Failed to parse memory size: {}", memoryStr, e);
            return DEFAULT_MEMORY;
        }
    }
}
