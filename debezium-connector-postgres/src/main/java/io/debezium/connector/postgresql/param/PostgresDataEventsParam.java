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

package io.debezium.connector.postgresql.param;

import io.debezium.connector.postgresql.PostgresOffsetContext;
import io.debezium.connector.postgresql.PostgresPartition;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.relational.RelationalSnapshotChangeEventSource.RelationalSnapshotContext;
import io.debezium.relational.Table;

/**
 * create postgresql DataEventsForTable into the reference entity
 *
 * @author jianghongbo
 * @since 2025/02/05
 */
public class PostgresDataEventsParam {
    private final ChangeEventSource.ChangeEventSourceContext sourceContext;
    private final RelationalSnapshotContext<PostgresPartition, PostgresOffsetContext> snapshotContext;
    private final EventDispatcher.SnapshotReceiver snapshotReceiver;
    private final Table table;
    private final boolean isLastTable;

    /**
     * Constructor
     *
     * @param sourceContext ChangeEventSource.ChangeEventSourceContext
     * @param snapshotContext RelationalSnapshotChangeEventSource.RelationalSnapshotContext<OpengaussPartition,
     *        OpengaussOffsetContext>
     * @param snapshotReceiver EventDispatcher.SnapshotReceiver
     * @param table table
     * @param isLastTable boolean
     */
    public PostgresDataEventsParam(ChangeEventSource.ChangeEventSourceContext sourceContext,
                                   RelationalSnapshotContext<PostgresPartition, PostgresOffsetContext> snapshotContext,
                                   EventDispatcher.SnapshotReceiver snapshotReceiver,
                                   Table table,
                                   boolean isLastTable) {
        this.sourceContext = sourceContext;
        this.snapshotContext = snapshotContext;
        this.snapshotReceiver = snapshotReceiver;
        this.table = table;
        this.isLastTable = isLastTable;
    }

    /**
     * Get ChangeEventSource ChangeEventSourceContext
     *
     * @return sourceContext
     */
    public ChangeEventSource.ChangeEventSourceContext getSourceContext() {
        return sourceContext;
    }

    /**
     * Get Snapshot Context
     *
     * @return snapshotContext RelationalSnapshotChangeEventSource.RelationalSnapshotContext
     */
    public RelationalSnapshotContext<PostgresPartition, PostgresOffsetContext> getSnapshotContext() {
        return snapshotContext;
    }

    /**
     * Get Snapshot Receiver
     *
     * @return snapshotReceiver
     */
    public EventDispatcher.SnapshotReceiver getSnapshotReceiver() {
        return snapshotReceiver;
    }

    /**
     * Get tableId
     *
     * @return TableId
     */
    public Table getTable() {
        return table;
    }

    /**
     * Get isLastTable
     *
     * @return isLastTable
     */
    public boolean isLastTable() {
        return isLastTable;
    }
}
