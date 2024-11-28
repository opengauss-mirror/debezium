/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
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
