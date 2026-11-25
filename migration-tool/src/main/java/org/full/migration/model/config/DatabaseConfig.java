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
