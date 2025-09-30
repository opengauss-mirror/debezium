/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package io.debezium.migration;

import org.apache.kafka.connect.data.Struct;

import java.util.Map;
import java.util.Objects;

/**
 * Description: EOF Event
 *
 * @author jianghongbo
 * @since 2025/02/05
 */
public class NotifyEvent {
    private final String database;
    private final Map<String, ?> partition;
    private final Map<String, ?> offset;
    private final Struct source;

    public NotifyEvent(Map<String, ?> partition, Map<String, ?> offset, Struct source, String database) {
        this.partition = Objects.requireNonNull(partition, "partition must not be null");
        this.offset = Objects.requireNonNull(offset, "offset must not be null");
        this.source = Objects.requireNonNull(source, "source must not be null");
        this.database = Objects.requireNonNull(database, "database must not be null");
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
}
