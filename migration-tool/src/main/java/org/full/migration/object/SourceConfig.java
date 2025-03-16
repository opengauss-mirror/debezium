/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.full.migration.object;

import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * SourceConfig
 *
 * @since 2025-03-15
 */
@Data
public class SourceConfig {
    private String type;
    private Integer readerNum;
    private Integer writerNum;
    private Integer retryNum;
    private DatabaseConfig dbConn;
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
    private Boolean containColumns;
    private String columnSplit;
    private Integer sleepLoop;
    private Integer indexParallelWorkers;
    private Boolean isKeepExistingSchema;
    private Boolean isMigrateDefaultValue;
    private Boolean isRestartConfig;
    private Boolean isCreateIndex;
    private String indexDir;
    private Boolean isSkipCompletedTables;
    private Boolean isWithDataCheck;
    private DataCheckConfig dataCheckConfig;
}
