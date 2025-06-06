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

import java.util.List;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

/**
 * GlobalConfig
 *
 * @since 2025-04-18
 */
@Data
public class GlobalConfig {
    private String pidDir;
    private String statusDir;
    private String logDir;
    private String logDest;
    private String logLevel;
    private String logDaysKeep;
    private String rollbarKey;
    private String rollbarEnv;
    @NotNull(message = "This parameter is required and of boolean type")
    private Boolean isDumpJson;
    @NotNull(message = "This parameter is required and of boolean type")
    private Boolean isDeleteCsv;
    @NotNull(message = "This parameter is required and of boolean type.")
    private Boolean isKeepExistingSchema;
    private AlertLogConfig alertLogConfig;
    private List<TypeOverride> typeOverrides;
    private CompressConfig compressConfig;
    @NotNull(message = "This parameter is required")
    @Valid
    private DatabaseConfig ogConn;
    @NotNull(message = "This parameter is required")
    @Valid
    private SourceConfig sourceConfig;
}
