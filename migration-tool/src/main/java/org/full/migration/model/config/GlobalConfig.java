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

package org.full.migration.model.config;

import lombok.Data;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.List;

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
    private Boolean isDeleteCsv;
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
    /**
     * 目标数据库类型，如：opengauss, ograc
     */
    private String targetType;

    private DataXParamConfig datax;
}
