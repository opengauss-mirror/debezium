/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package io.debezium.connector.postgresql;

import io.debezium.data.Envelope;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.RelationalChangeRecordEmitter;
import io.debezium.util.Clock;

/**
 * Description: emitter for create index record
 *
 * @author jianghongbo
 * @since 2025/02/05
 */
public class SnapshotCreateIndexRecordEmitter extends RelationalChangeRecordEmitter {
    /**
     * Constructor
     *
     * @param partition partition
     * @param offsetContext offsetContext
     * @param clock clock
     * @param idxDdl create index statement
     */
    public SnapshotCreateIndexRecordEmitter(Partition partition, OffsetContext offsetContext,
                                            Clock clock, String idxDdl) {
        super(partition, offsetContext, clock, idxDdl);
    }

    @Override
    protected Envelope.Operation getOperation() {
        return Envelope.Operation.CREATE_INDEX;
    }

    @Override
    protected Object[] getOldColumnValues() {
        return null;
    }

    @Override
    protected Object[] getNewColumnValues() {
        return null;
    }
}
