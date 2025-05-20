/*
 * Copyright (c) 2025-2025 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *           http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.full.migration.model.config;

import lombok.Data;

import org.apache.commons.lang3.StringUtils;
import org.full.migration.constants.Unit;
import org.full.migration.validator.ValidMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

/**
 * SourceConfig
 *
 * @since 2025-04-18
 */
@Data
public class SourceConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(SourceConfig.class);
    private static final int MEMORY_UNIT = 1024;
    private static final BigInteger MEMORY_UNIT_BIGINTEGER = BigInteger.valueOf(MEMORY_UNIT);
    private static final BigInteger DEFAULT_PAGE_SIZE = BigInteger.valueOf(2 * MEMORY_UNIT * MEMORY_UNIT);
    private static final Pattern SIZE_PATTERN = Pattern.compile("^\\d+[kKmMgG]?$");

    private String type;
    @NotNull(message = "This parameter is required")
    @Min(value = 1, message = "number must larger than 0")
    private Integer readerNum;
    @NotNull(message = "This parameter is required")
    @Min(value = 1, message = "number must larger than 0")
    private Integer writerNum;
    private Integer retryNum;
    private String fileSize;
    @NotNull(message = "This parameter is required")
    private DatabaseConfig dbConn;
    @NotNull(message = "This parameter is required")
    @ValidMap
    private Map<String, String> schemaMappings;
    private List<String> limitTables;
    private List<String> skipTables;
    private Boolean isCompress;
    private List<String> compressTables;
    private List<String> grantRoles;
    private String lockTimeout;
    private Integer serverId;
    private Integer replicaBatchSize;
    private Integer replayMaxRows;
    private String batchRetention;
    private String copyMaxMemory;
    private String copyMode;
    private String outDir;
    private String csvDir;
    private Boolean isTimeMigrate;
    private Boolean isMoneyMigrate;
    private Boolean containColumns;
    private String columnSplit;
    private Integer sleepLoop;
    private Integer indexParallelWorkers;
    private Boolean isMigrateDefaultValue;
    @NotNull(message = "This parameter is required")
    private Boolean isRecordSnapshot;
    private Boolean isRestartConfig;
    private Boolean isCreateIndex;
    private String indexDir;
    private Boolean isSkipCompletedTables;
    private Boolean isWithDataCheck;
    private DataCheckConfig dataCheckConfig;

    /**
     * convertFileSize
     *
     * @return BigInteger
     */
    public BigInteger convertFileSize() {
        if (StringUtils.isEmpty(fileSize)) {
            LOGGER.warn("pageSize is empty. Default value: {} byte", DEFAULT_PAGE_SIZE);
            return DEFAULT_PAGE_SIZE;
        }
        if (!SIZE_PATTERN.matcher(fileSize).matches()) {
            LOGGER.warn("Invalid pageSize format: {}. Default value: {} byte", fileSize, DEFAULT_PAGE_SIZE);
            return DEFAULT_PAGE_SIZE;
        }
        if (StringUtils.isNumeric(fileSize)) {
            return Unit.calculateSize(BigInteger.valueOf(Integer.parseInt(fileSize)), Unit.B);
        }
        return initStoreSize(fileSize);
    }

    /**
     * initStoreSize
     *
     * @param sizeStr sizeStr
     * @return BigInteger
     */
    public BigInteger initStoreSize(String sizeStr) {
        char unitChar = sizeStr.toUpperCase(Locale.ROOT).charAt(sizeStr.length() - 1);
        int size = Integer.parseInt(sizeStr.substring(0, sizeStr.length() - 1));
        try {
            Unit unit = Unit.valueOf(String.valueOf(unitChar));
            return Unit.calculateSize(BigInteger.valueOf(size), unit);
        } catch (IllegalArgumentException e) {
            LOGGER.warn("Invalid unit in pageSize: {}. Default value: {} byte", sizeStr, DEFAULT_PAGE_SIZE);
            return DEFAULT_PAGE_SIZE;
        }
    }

    /**
     * isValid
     *
     * @param taskType taskType
     * @return isValid
     */
    public boolean isValid(String taskType) {
        if ("table".equalsIgnoreCase(taskType)) {
            return isCsvDirValid(csvDir) && Objects.nonNull(isTimeMigrate);
        }
        return true;
    }

    private boolean isCsvDirValid(String csvDir) {
        return StringUtils.isNotBlank(csvDir);
    }
}
