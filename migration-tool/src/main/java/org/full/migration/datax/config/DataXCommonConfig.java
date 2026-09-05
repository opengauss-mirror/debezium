/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

package org.full.migration.datax.config;

import lombok.Data;

/**
 * DataXCommonConfig
 * Common configuration for DataX jobs.
 * This class holds all the configuration parameters needed for DataX migration tasks,
 * including channel settings, batch sizes, timeout values, retry configurations,
 * and reader/writer connection parameters.
 *
 * <p>Configuration parameters include:</p>
 * <ul>
 *   <li>channel - Number of concurrent channels for data transfer (default: 3)</li>
 *   <li>errorRecordLimit - Maximum number of error records allowed (default: 0)</li>
 *   <li>errorPercentageLimit - Maximum error percentage allowed (default: 0.02)</li>
 *   <li>readBatchSize - Number of records to read per batch (default: 1024)</li>
 *   <li>writeBatchSize - Number of records to write per batch (default: 1024)</li>
 *   <li>readTimeout - Read timeout in milliseconds (default: 60000)</li>
 *   <li>writeTimeout - Write timeout in milliseconds (default: 60000)</li>
 *   <li>enableBatchWrite - Whether to enable batch write mode (default: true)</li>
 *   <li>batchWriteSize - Number of records per batch write (default: 1000)</li>
 *   <li>enablePrepareStatement - Whether to use prepared statements (default: true)</li>
 *   <li>retryTimes - Number of retry attempts on failure (default: 3)</li>
 *   <li>retryInterval - Interval between retries in milliseconds (default: 1000)</li>
 *   <li>minCpuCoresForHighPerformance - Minimum CPU cores for high-performance strategy (default: 8)</li>
 *   <li>minMemoryForHighPerformance - Minimum physical memory (GB) for high-performance strategy (default: 16)</li>
 *   <li>maxChannels - Maximum number of channels per DataX job (default: 32)</li>
 * </ul>
 *
 * @since 2025-04-18
 */
@Data
public class DataXCommonConfig {
    private int channel = 3;
    private int errorRecordLimit = 100;
    private double errorPercentageLimit = 0.02;
    private int readBatchSize = 1024;
    private int writeBatchSize = 1024;
    private int readTimeout = 60000;
    private int writeTimeout = 60000;
    private boolean enableBatchWrite = true;
    private int batchWriteSize = 1024;
    private boolean enablePrepareStatement = true;
    private int retryTimes = 3;
    private int retryInterval = 10000;
    private String readerName="oraclereader";
    private String readerJdbcUrl;
    private String readerUsername;
    private String readerPassword;

    private String writerName="gaussdbwriter";
    private String writerUsername;
    private String writerPassword;
    private String writerJdbcUrl;
    
    private int minCpuCoresForHighPerformance = 8;
    private long minMemoryForHighPerformance = 16L;
    
    private int maxChannels = 32;
}