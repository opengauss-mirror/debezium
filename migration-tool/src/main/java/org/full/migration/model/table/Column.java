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

package org.full.migration.model.table;

import lombok.Builder;
import lombok.Data;

import java.util.List;

/**
 * Column
 *
 * @since 2025-04-18
 */
@Data
@Builder
public class Column {
    private String name;
    private int position;
    private int jdbcType;
    private int nativeType;
    private String typeName;
    private String typeExpression;
    private String charsetName;
    private long length;
    private Integer scale;
    private boolean optional;
    private boolean autoIncremented;
    private boolean generated;
    private String defaultValueExpression;
    private boolean hasDefaultValue;
    private List<String> enumValues;
    private List<String> modifyKeys;
    private String comment;
    private String intervalType;
}
