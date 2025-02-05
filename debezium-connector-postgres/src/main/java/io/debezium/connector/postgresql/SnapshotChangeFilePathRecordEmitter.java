/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import java.util.Arrays;
import java.util.List;

import org.apache.kafka.connect.data.Struct;

import io.debezium.data.Envelope;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.RelationalChangeRecordEmitter;
import io.debezium.relational.TableSchema;
import io.debezium.util.Clock;

/**
 * Description: The entity that logs messages during full migration
 *
 * @author jhb
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
        super(partition, offsetContext, clock, null);
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

    @Override
    protected void emitPathRecord(Receiver receiver, TableSchema tableSchema)
            throws InterruptedException {
        Object[] values = getNewColumnValues();
        tableSchema.removeKeySchema();
        // value: [path, subscript, totalSlice, spacePerSlice]
        List<Object> valueList = Arrays.asList(values);
        assert valueList.size() == 4;
        String data = valueList.get(0) + "|" + valueList.get(1);
        Struct envelope = tableSchema.getEnvelopeSchema().path(data, null, getOffset().getSourceInfo(),
                getClock().currentTimeAsInstant(), valueList.get(2), valueList.get(3));
        receiver.changeRecord(getPartition(), tableSchema, Envelope.Operation.PATH, null, envelope, getOffset(), null);
    }
}
