/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package io.debezium.connector.postgresql.sink.record;

import java.util.List;

import org.apache.kafka.connect.data.Struct;

/**
 * This class is a placeholder for the actual functionality that needs to be implemented.
 *
 * @author tianbin
 * @since 2024/11/14
 */
public class SinkMetadataRecord {
    private String schemaName;
    private String tableName;
    private List<Struct> tableChanges;
    private String parents;
    private String partition;

    public SinkMetadataRecord(String schemaName, String tableName, List<Struct> tableChanges,
                              String parents, String partition) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.tableChanges = tableChanges;
        this.parents = parents;
        this.partition = partition;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void setTableChanges(List<Struct> tableChanges) {
        this.tableChanges = tableChanges;
    }

    public void setParents(String parents) {
        this.parents = parents;
    }

    public void setPartition(String partition) {
        this.partition = partition;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public List<Struct> getTableChanges() {
        return tableChanges;
    }

    public String getParents() {
        return parents;
    }

    public String getPartition() {
        return partition;
    }
}
