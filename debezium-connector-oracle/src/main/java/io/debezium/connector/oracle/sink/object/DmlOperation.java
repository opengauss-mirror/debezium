/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.sink.object;

import org.apache.kafka.connect.data.Struct;

/**
 * Description: Dml operation class
 * @author gbase
 * @date 2023/07/28
 **/
public class DmlOperation extends DataOperation {
    /**
     * Before
     */
    public static final String BEFORE = "before";

    /**
     * After
     */
    public static final String AFTER = "after";

    /**
     * Operation
     */
    public static final String OPERATION = "op";

    /**
     * Transaction
     */
    public static final String TRANSACTION = "transaction";

    /**
     * Transaction id
     */
    public static final String ID = "id";

    /**
     * Timestamp_milliseconds
     */
    public static final String TIMESTAMP_MS = "ts_ms";

    private Struct before;
    private Struct after;
    private String operation;
    private String transactionId;
    private String timestamp;

    /**
     * Constructor
     *
     * @param value the value
     */
    public DmlOperation(Struct value) {
        if (value == null) {
            throw new IllegalArgumentException("value can't be null!");
        }
        this.operation = value.getString(DmlOperation.OPERATION);
        this.before = value.getStruct(DmlOperation.BEFORE);
        this.after = value.getStruct(DmlOperation.AFTER);
        if (value.getStruct(DmlOperation.TRANSACTION) != null) {
            this.transactionId = value.getStruct(DmlOperation.TRANSACTION)
                    .getString(DmlOperation.ID);
        }
        setIsDml(true);
    }

    /**
     * Gets before value
     *
     * @return Struct the before value
     */
    public Struct getBefore() {
        return before;
    }

    /**
     * Sets before value
     *
     * @param before the before value
     */
    public void setBefore(Struct before) {
        this.before = before;
    }

    /**
     * Gets after value
     *
     * @return Struct the after value
     */
    public Struct getAfter() {
        return after;
    }

    /**
     * Sets after value
     *
     * @param after the after value
     */
    public void setAfter(Struct after) {
        this.after = after;
    }

    /**
     * Gets operation
     *
     * @return String the operation
     */
    public String getOperation() {
        return operation;
    }

    /**
     * Sets operation
     *
     * @param operation the operation
     */
    public void setOperation(String operation) {
        this.operation = operation;
    }

    /**
     * Gets transaction
     *
     * @return String the transaction
     */
    public String getTransactionId() {
        return transactionId;
    }

    /**
     * Sets transaction
     *
     * @param transactionId the transaction id
     */
    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    /**
     * Gets timestamp
     *
     * @return String the timestamp
     */
    public String getTimestamp() {
        return timestamp;
    }

    /**
     * Sets timestamp
     *
     * @param timestamp the timestamp
     */
    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "DmlOperation{" +
                "isDml=" + isDml +
                ", before=" + before +
                ", after=" + after +
                ", operation='" + operation + '\'' +
                ", transactionId='" + transactionId + '\'' +
                ", timestamp='" + timestamp + '\'' +
                '}';
    }
}
