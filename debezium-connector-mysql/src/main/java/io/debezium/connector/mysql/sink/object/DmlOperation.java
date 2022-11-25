/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.sink.object;

import org.apache.kafka.connect.data.Struct;

/**
 * Description: DmlField class
 * @author douxin
 * @date 2022/10/28
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
     * Timestamp_milliseconds
     */
    public static final String TIMESTAMP_MS = "ts_ms";

    private Struct before;
    private Struct after;
    private String operation;
    private String Transaction;
    private String timestamp;

    /**
     * Constructor
     *
     * @param Struct the value
     */
    public DmlOperation(Struct value) {
        if (value == null) {
            throw new IllegalArgumentException("value can't be null!");
        }
        this.operation = value.getString(DmlOperation.OPERATION);
        this.before = value.getStruct(DmlOperation.BEFORE);
        this.after = value.getStruct(DmlOperation.AFTER);
        setIsDml(true);
    }

    /**
     * Constructor
     *
     * @param Struct the before value
     * @param Struct the after value
     * @param Struct the operation
     */
    public DmlOperation(Struct before, Struct after, String operation) {
        this.before = before;
        this.after = after;
        this.operation = operation;
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
     * @param Struct the before value
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
     * @param Struct the after value
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
     * @param String the operation
     */
    public void setOperation(String operation) {
        this.operation = operation;
    }

    /**
     * Gets transaction
     *
     * @return String the transaction
     */
    public String getTransaction() {
        return Transaction;
    }

    /**
     * Sets transaction
     *
     * @param String the transaction
     */
    public void setTransaction(String transaction) {
        Transaction = transaction;
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
     * @param String the timestamp
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
                ", Transaction='" + Transaction + '\'' +
                ", timestamp='" + timestamp + '\'' +
                '}';
    }
}
