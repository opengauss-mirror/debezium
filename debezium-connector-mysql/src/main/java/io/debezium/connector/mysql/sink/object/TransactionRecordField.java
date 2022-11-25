/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.sink.object;

/**
 * Description: TransactionRecordField class
 *
 * @author douxin
 * @date 2022/11/24
 **/
public class TransactionRecordField {
    /**
     * Status
     */
    public static final String STATUS = "status";

    /**
     * Id
     */
    public static final String ID = "id";

    /**
     * Event_count
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
