/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.opengauss;

import java.util.Optional;

import io.debezium.connector.opengauss.connection.OpengaussConnection;
import io.debezium.connector.opengauss.connection.ReplicationConnection;
import io.debezium.connector.opengauss.spi.SlotCreationResult;
import io.debezium.connector.opengauss.spi.SlotState;
import io.debezium.connector.opengauss.spi.Snapshotter;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.schema.DataCollectionId;
import io.debezium.util.Clock;
import io.debezium.util.Strings;

public class OpengaussChangeEventSourceFactory implements ChangeEventSourceFactory<OpengaussPartition, OpengaussOffsetContext> {

    private final OpengaussConnectorConfig configuration;
    private final OpengaussConnection jdbcConnection;
    private final ErrorHandler errorHandler;
    private final OpengaussEventDispatcher<TableId> dispatcher;
    private final Clock clock;
    private final OpengaussSchema schema;
    private final OpengaussTaskContext taskContext;
    private final Snapshotter snapshotter;
    private final ReplicationConnection replicationConnection;
    private final SlotCreationResult slotCreatedInfo;
    private final SlotState startingSlotInfo;

    public OpengaussChangeEventSourceFactory(OpengaussConnectorConfig configuration, Snapshotter snapshotter, OpengaussConnection jdbcConnection,
                                             ErrorHandler errorHandler, OpengaussEventDispatcher<TableId> dispatcher, Clock clock, OpengaussSchema schema,
                                             OpengaussTaskContext taskContext,
                                             ReplicationConnection replicationConnection, SlotCreationResult slotCreatedInfo, SlotState startingSlotInfo) {
        this.configuration = configuration;
        this.jdbcConnection = jdbcConnection;
        this.errorHandler = errorHandler;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.schema = schema;
        this.taskContext = taskContext;
        this.snapshotter = snapshotter;
        this.replicationConnection = replicationConnection;
        this.slotCreatedInfo = slotCreatedInfo;
        this.startingSlotInfo = startingSlotInfo;
    }

    @Override
    public SnapshotChangeEventSource<OpengaussPartition, OpengaussOffsetContext> getSnapshotChangeEventSource(SnapshotProgressListener snapshotProgressListener) {
        return new OpengaussSnapshotChangeEventSource(
                configuration,
                snapshotter,
                jdbcConnection,
                schema,
                dispatcher,
                clock,
                snapshotProgressListener,
                slotCreatedInfo,
                startingSlotInfo);
    }

    @Override
    public StreamingChangeEventSource<OpengaussPartition, OpengaussOffsetContext> getStreamingChangeEventSource() {
        return new OpengaussStreamingChangeEventSource(
                configuration,
                snapshotter,
                jdbcConnection,
                dispatcher,
                errorHandler,
                clock,
                schema,
                taskContext,
                replicationConnection);
    }

    @Override
    public Optional<IncrementalSnapshotChangeEventSource<? extends DataCollectionId>> getIncrementalSnapshotChangeEventSource(
                                                                                                                              OpengaussOffsetContext offsetContext,
                                                                                                                              SnapshotProgressListener snapshotProgressListener,
                                                                                                                              DataChangeEventListener dataChangeEventListener) {
        // If no data collection id is provided, don't return an instance as the implementation requires
        // that a signal data collection id be provided to work.
        if (Strings.isNullOrEmpty(configuration.getSignalingDataCollectionId())) {
            return Optional.empty();
        }
        final OpengaussSignalBasedIncrementalSnapshotChangeEventSource incrementalSnapshotChangeEventSource = new OpengaussSignalBasedIncrementalSnapshotChangeEventSource(
                configuration,
                jdbcConnection,
                dispatcher,
                schema,
                clock,
                snapshotProgressListener,
                dataChangeEventListener);
        return Optional.of(incrementalSnapshotChangeEventSource);
    }
}
