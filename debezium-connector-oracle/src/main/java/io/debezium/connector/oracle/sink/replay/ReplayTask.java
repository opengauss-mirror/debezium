/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.sink.replay;

import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Description: Base task class
 *
 * @author gbase
 * @since 2023-07-28
 **/
public class ReplayTask {
    /**
     * Sink queue, storing kafka records
     */
    protected BlockingQueue<SinkRecord> sinkQueue = new LinkedBlockingQueue<>();

    /**
     * Create work threads
     */
    public void createWorkThreads() {
    }

    /**
     * Batch write
     *
     * @param records Collection<SinkRecord> the records
     */
    public void batchWrite(Collection<SinkRecord> records) {
        sinkQueue.addAll(records);
    }

    /**
     * Do stop method
     */
    public void doStop() {
    }

    /**
     * Is block
     *
     * @return true if is block
     */
    public boolean isBlock() {
        return false;
    }
}
