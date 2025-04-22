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

import org.full.migration.validator.ValidIP;

import java.util.Map;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

/**
 * DatabaseConfig
 *
 * @since 2025-04-18
 */
@Data
public class DatabaseConfig {
    @ValidIP
    private String host;
    @NotNull(message = "This parameter is required")
    @Min(value = 1, message = "port number must larger than 0")
    @Max(value = 65535, message = "port number must smaller than 65536")
    private Integer port;
    @NotNull(message = "This parameter is required")
    private String user;
    @NotNull(message = "This parameter is required")
    private String password;
    @NotNull(message = "This parameter is required")
    private String database;
    private String schema;
    private String charset;
    private Integer connectTimeout;
    private Map<String, String> params;
}
