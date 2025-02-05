/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package io.debezium.migration;

import java.util.Map;
import java.util.Objects;

import org.apache.kafka.connect.data.Struct;

/**
 * Description: ObjectChangeEvent
 *
 * @author jianghongbo
 * @since 2025/02/05
 */
public class ObjectChangeEvent {
    private final String database;
    private final String schema;
    private final String ddl;
    private final String objName;
    private final String objType;
    private final Map<String, ?> partition;
    private final Map<String, ?> offset;
    private final Struct source;

    public ObjectChangeEvent(Map<String, ?> partition, Map<String, ?> offset, Struct source, String database,
                             String schema, String ddl, String objName, ObjectEnum objType) {
        this.partition = Objects.requireNonNull(partition, "partition must not be null");
        this.offset = Objects.requireNonNull(offset, "offset must not be null");
        this.source = Objects.requireNonNull(source, "source must not be null");
        this.database = Objects.requireNonNull(database, "database must not be null");
        this.schema = schema;
        this.ddl = ddl;
        this.objName = objName;
        this.objType = objType.code();
    }

    public Map<String, ?> getPartition() {
        return partition;
    }

    public Map<String, ?> getOffset() {
        return offset;
    }

    public Struct getSource() {
        return source;
    }

    public String getDatabase() {
        return database;
    }

    public String getSchema() {
        return schema;
    }

    public String getDdl() {
        return ddl;
    }

    public String getObjName() {
        return objName;
    }
    public String getObjType() {
        return objType;
    }
}
