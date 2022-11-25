/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.sink.event;

import com.github.shyiko.mysql.binlog.event.GtidEventData;

/**
 * Description: MyGtidEventData class
 * @author douxin
 * @date 2022/10/21
 **/
public class MyGtidEventData extends GtidEventData {
    private long lastCommitted;
    private long sequenceNumber;

    /**
     * Sets last_committed
     *
     * @param long the last_committed
     */
    public void setLastCommitted(long lastCommitted) {
        this.lastCommitted = lastCommitted;
    }

    /**
     * Gets last_committed
     *
     * @return long the last_committed
     */
    public long getLastCommitted() {
        return this.lastCommitted;
    }

    /**
     * Sets sequence_number
     *
     * @param long the sequence_number
     */
    public void setSequenceNumber(long sequenceNumber) {
        this.sequenceNumber = sequenceNumber;
    }

    /**
     * Gets sequence_number
     *
     * @return long the sequence_number
     */
    public long getSequenceNumber() {
        return this.sequenceNumber;
    }

    /**
     * To string method
     *
     * @return String the string
     */
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("GtidEventData");
        sb.append("{flags=").append(getFlags()).append(", gtid='").append(getGtid()).append('\'');
        sb.append(", lastCommitted = ").append(lastCommitted);
        sb.append(", sequenceNumber = ").append(sequenceNumber);
        sb.append('}');
        return sb.toString();
    }
}
