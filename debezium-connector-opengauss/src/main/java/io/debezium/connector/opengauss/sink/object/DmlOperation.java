/**
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.opengauss.sink.object;

import org.apache.kafka.connect.data.Struct;

/**
 * Description: Dml operation class
 * @author wangzhengyuan
 * @date 2022/11/04
 */
public class DmlOperation {
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

    private Struct before;
    private Struct after;
    private String operation;

    /**
     * Constructor
     *
     * @param value Struct the value
     */
    public DmlOperation(Struct value){
        this.operation = value.getString(DmlOperation.OPERATION);
        this.before = value.getStruct(DmlOperation.BEFORE);
        this.after = value.getStruct(DmlOperation.AFTER);
    }

    /**
     * Gets before
     *
     * @return Struct the before
     */
    public Struct getBefore() {
        return before;
    }

    /**
     * Sets before
     *
     * @param before Struct the before
     */
    public void setBefore(Struct before) {
        this.before = before;
    }

    /**
     * Gets after
     *
     * @return Struct the after
     */
    public Struct getAfter() {
        return after;
    }

    /**
     * Sets after
     *
     * @param after Struct the after
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
     * @param operation String the operation
     */
    public void setOperation(String operation) {
        this.operation = operation;
    }
}