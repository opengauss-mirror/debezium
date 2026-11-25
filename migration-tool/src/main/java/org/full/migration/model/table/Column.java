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
    private GenerateInfo generateInfo;
    private String defaultValueExpression;
    private boolean hasDefaultValue;
    private List<String> enumValues;
    private List<String> modifyKeys;
    private String comment;
    private String intervalType;
   
    /**
     * char_used property for Oracle
     * Used to store CHAR_USED information for CHAR, NCHAR, VARCHAR2 types in Oracle database
     * For example: CHAR(10 char) CHAR_USED char
     * For: VARCHAR2(10 byte) CHAR_USED byte
     */
    private String charUsed;
}
