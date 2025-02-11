/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package io.debezium.connector.postgresql.param;

import io.debezium.connector.postgresql.PostgresOffsetContext;
import io.debezium.connector.postgresql.PostgresPartition;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.relational.RelationalSnapshotChangeEventSource.RelationalSnapshotContext;
import io.debezium.relational.TableId;

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
    private final TableId tableId;
    private final boolean isLastTable;

    /**
     * Constructor
     *
     * @param sourceContext ChangeEventSource.ChangeEventSourceContext
     * @param snapshotContext RelationalSnapshotChangeEventSource.RelationalSnapshotContext<OpengaussPartition,
     *        OpengaussOffsetContext>
     * @param snapshotReceiver EventDispatcher.SnapshotReceiver
     * @param tableId TableId
     * @param isLastTable boolean
     */
    public PostgresDataEventsParam(ChangeEventSource.ChangeEventSourceContext sourceContext,
                                   RelationalSnapshotContext<PostgresPartition, PostgresOffsetContext> snapshotContext,
                                   EventDispatcher.SnapshotReceiver snapshotReceiver,
                                   TableId tableId,
                                   boolean isLastTable) {
        this.sourceContext = sourceContext;
        this.snapshotContext = snapshotContext;
        this.snapshotReceiver = snapshotReceiver;
        this.tableId = tableId;
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
    public TableId getTableId() {
        return tableId;
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
