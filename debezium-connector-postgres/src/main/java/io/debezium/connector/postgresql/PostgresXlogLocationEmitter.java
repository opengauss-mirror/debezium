/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package io.debezium.connector.postgresql;

import org.apache.kafka.connect.data.Struct;

import io.debezium.data.Envelope;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.RelationalChangeRecordEmitter;
import io.debezium.relational.TableSchema;
import io.debezium.util.Clock;

/**
 * Description: PostgresXlogLocationEmitter
 *
 * @author jianghongbo
 * @since 2025/02/05
 */
public class PostgresXlogLocationEmitter extends RelationalChangeRecordEmitter {
    private String xlogLocation;

    /**
     * Constructor
     *
     * @param partition Partition
     * @param offsetContext OffsetContext
     * @param clock Clock
     * @param xlogLocation String
     */
    public PostgresXlogLocationEmitter(Partition partition, OffsetContext offsetContext,
                                       Clock clock, String xlogLocation) {
        super(partition, offsetContext, clock);
        this.xlogLocation = xlogLocation;
    }

    public String getXlogLocation() {
        return xlogLocation;
    }

    @Override
    protected Envelope.Operation getOperation() {
        return Envelope.Operation.TABLE_SNAPSHOT;
    }

    @Override
    protected Object[] getOldColumnValues() {
        return null;
    }

    @Override
    protected Object[] getNewColumnValues() {
        return null;
    }

    /**
     * emit xlog location to kafka
     *
     * @param receiver Receiver
     * @param tableSchema TableSchema
     * @throws InterruptedException changeRecord may throw
     */
    protected void emitSnapshotRecord(Receiver receiver, TableSchema tableSchema) throws InterruptedException {
        tableSchema.removeKeySchema();
        Struct envelope = tableSchema.getEnvelopeSchema().snapshot(xlogLocation, getOffset().getSourceInfo(),
                getClock().currentTimeAsInstant());
        receiver.changeRecord(getPartition(), tableSchema, Envelope.Operation.TABLE_SNAPSHOT, null, envelope,
                getOffset(), null);
    }
}
