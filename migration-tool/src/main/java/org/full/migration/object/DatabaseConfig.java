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

import java.util.Map;

/**
 * DatabaseConfig
 *
 * @since 2025-03-15
 */
@Data
public class DatabaseConfig {
    private String host;
    private Integer port;
    private String user;
    private String password;
    private String database;
    private String schema;
    private String charset;
    private Integer connectTimeout;
    private Map<String, String> params;
}
