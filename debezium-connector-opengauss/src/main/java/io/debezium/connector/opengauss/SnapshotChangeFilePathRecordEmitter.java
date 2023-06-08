/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.opengauss;

import io.debezium.data.Envelope;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.RelationalChangeRecordEmitter;
import io.debezium.util.Clock;

/**
 * Description: The entity that logs messages during full migration
 *
 * @author czy
 * @date 2023/06/02
 */
public class SnapshotChangeFilePathRecordEmitter extends RelationalChangeRecordEmitter {
    // Contains two elements, the first is file location information, and the second is table column information
    private final Object[] row;

    /**
     * Constructor
     *
     * @param partition partition
     * @param offsetContext offsetContext
     * @param clock clock
     * @param row data
     */
    public SnapshotChangeFilePathRecordEmitter(Partition partition, OffsetContext offsetContext,
        Clock clock, Object[] row) {
        super(partition, offsetContext, clock);
        this.row = row;
    }

    @Override
    protected Envelope.Operation getOperation() {
        return Envelope.Operation.PATH;
    }

    @Override
    protected Object[] getOldColumnValues() {
        throw new UnsupportedOperationException("can't get old row values for Path read.");
    }

    @Override
    protected Object[] getNewColumnValues() {
        return row;
    }
}
