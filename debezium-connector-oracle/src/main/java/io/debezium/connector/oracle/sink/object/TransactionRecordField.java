/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.sink.object;

/**
 * Description: TransactionRecordField class
 *
 * @author gbase
 * @date 2023/07/28
 **/
public class TransactionRecordField {
    /**
     * Status, BEGIN or END
     */
    public static final String STATUS = "status";

    /**
     * String representation of unique transaction identifier
     */
    public static final String ID = "id";

    /**
     * Total number of events emitted by the transaction.
     */
    public static final String EVENT_COUNT = "event_count";

    /**
     * Begin
     */
    public static final String BEGIN = "BEGIN";

    /**
     * End
     */
    public static final String END = "END";
}
