/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package io.debezium.connector.postgresql.sink.object;

import org.apache.kafka.connect.data.Struct;

/**
 * This class provides methods to perform data manipulation language (DML) operations.
 * It includes methods for inserting, updating, and deleting records in a database.
 *
 * @author tianbin
 * @since 2024/12/04
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
    public DmlOperation(Struct value) {
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