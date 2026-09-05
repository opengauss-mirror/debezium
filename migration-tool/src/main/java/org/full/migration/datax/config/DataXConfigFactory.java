/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

package org.full.migration.datax.config;

import java.lang.management.ManagementFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.management.OperatingSystemMXBean;

/**
 * DataXConfigFactory
 * DataX configuration factory class, used to manage configuration strategies
 * Supports automatic strategy selection based on machine resources and table size
 *
 * @since 2025-04-18
 */
public class DataXConfigFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataXConfigFactory.class);
    private static final DataXConfigFactory INSTANCE = new DataXConfigFactory();
    
    private final DataXConfigStrategy generalStrategy;
    private final DataXConfigStrategy highPerformanceStrategy;

    private volatile Boolean machineHighPerformanceCapable;

    private DataXConfigFactory() {
        generalStrategy = new GeneralDataXConfigStrategy();
        highPerformanceStrategy = new HighPerformanceDataXConfigStrategy();
        LOGGER.info("DataXConfigFactory initialized with strategies: {} and {}", 
                generalStrategy.getStrategyName(), highPerformanceStrategy.getStrategyName());
    }
    
    /**
     * Get the singleton instance of DataXConfigFactory
     * @return The singleton instance of DataXConfigFactory 
     */
    public static DataXConfigFactory getInstance() {
        return INSTANCE;
    }
    
    /**
     * Get the applicable configuration strategy for the given context.
     * Always uses automatic detection based on machine resources and table size.
     *
     * @param context Configuration context
     * @return The applicable configuration strategy
     */
    public DataXConfigStrategy getApplicableStrategy(DataXConfigContext context) {
        DataXConfigStrategy detectedStrategy = detectOptimalStrategy(context);
        LOGGER.info("Auto-detected strategy: {} for table {}",
                    detectedStrategy.getStrategyName(), context.getTableName());
        return detectedStrategy;
    }
    
    /**
     * Detect the optimal strategy based on machine resources and table size.
     * Small tables (less than 1 million rows) use general strategy even on high-performance machines.
     *
     * @param context Configuration context with table information
     * @return The optimal strategy
     */
    private DataXConfigStrategy detectOptimalStrategy(DataXConfigContext context) {
        DataXCommonConfig commonConfig = context.getCommonConfig();
        
        if(commonConfig == null) {
            return generalStrategy;
        }

        if (!isMachineHighPerformanceCapable(commonConfig)) {
            return generalStrategy;
        }

        if (context.getTable() == null) {
            return generalStrategy;
        }

        long rowCount = context.getTable().getEstimatedRowCount();
        if (rowCount < 1_000_000) {
            LOGGER.info("Table {} has {} rows, using general strategy for small table",
                    context.getTableName(), rowCount);
            return generalStrategy;
        }

        return highPerformanceStrategy;
    }

    /**
     * Check if the machine meets high performance criteria, with DCL caching.
     *
     * @param commonConfig Common configuration with thresholds
     * @return true if the machine is capable of high performance mode
     */
    private boolean isMachineHighPerformanceCapable(DataXCommonConfig commonConfig) {
        if (machineHighPerformanceCapable != null) {
            return machineHighPerformanceCapable;
        }
        synchronized (this) {
            if (machineHighPerformanceCapable != null) {
                return machineHighPerformanceCapable;
            }
            int availableCores = Runtime.getRuntime().availableProcessors();
            long availableMemoryGB = getPhysicalMemoryGB();
            LOGGER.info("Detecting machine resources - CPU cores: {}, Physical Memory: {}GB",
                    availableCores, availableMemoryGB);

            int minCpuCores = commonConfig.getMinCpuCoresForHighPerformance();
            long minMemoryGB = commonConfig.getMinMemoryForHighPerformance();

            machineHighPerformanceCapable = availableCores >= minCpuCores && availableMemoryGB >= minMemoryGB;
            LOGGER.info("Machine high performance capable: {} (CPU: {} >= {}, Memory: {}GB >= {}GB)",
                    machineHighPerformanceCapable, availableCores, minCpuCores, availableMemoryGB, minMemoryGB);
            return machineHighPerformanceCapable;
        }
    }

    /**
     * Get physical memory in GB.
     * Uses com.sun.management.OperatingSystemMXBean for accurate physical memory detection,
     * falls back to JVM max heap memory if the MXBean is unavailable.
     *
     * @return Physical memory in GB
     */
    private static long getPhysicalMemoryGB() {
        Object osBean = ManagementFactory.getOperatingSystemMXBean();
        if (osBean instanceof OperatingSystemMXBean) {
            return ((OperatingSystemMXBean) osBean).getTotalMemorySize() / (1024 * 1024 * 1024);
        }
        LOGGER.debug("OperatingSystemMXBean not available, falling back to JVM max memory");
        return Runtime.getRuntime().maxMemory() / (1024 * 1024 * 1024);
    }
}
