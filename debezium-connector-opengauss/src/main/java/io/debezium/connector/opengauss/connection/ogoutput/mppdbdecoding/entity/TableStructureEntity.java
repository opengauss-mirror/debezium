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
package io.debezium.connector.opengauss.connection.ogoutput.mppdbdecoding.entity;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

/**
 * TableStructureEntity
 *
 * @since 2024-12-11
 */
@Getter
@Setter
@NoArgsConstructor
public class TableStructureEntity {
    private String schemaName;
    private String tableName;
    private char tupleType;
    private short attrNum;
    private List<String> columnName;
    private List<Integer> oid;
    private List<String> colValue;

    @Override
    public String toString() {
    return "TableStructureEntity{" + "schemaName='" + schemaName + '\'' + ", tableName='" + tableName + '\''
        + ", tupleType=" + tupleType + ", attrNum=" + attrNum + ", columnName=" + columnName + ", oid=" + oid
        + ", colValue=" + colValue + '}';
    }
}
