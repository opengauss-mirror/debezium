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

import io.debezium.data.Envelope;

/**
 * Dml operation class
 *
 * @author tianbin
 * @since 2024/12/04
 */
public class DataReplayOperation {
    /**
     * Before
     */
    public static final String BEFORE = Envelope.FieldName.BEFORE;

    /**
     * After
     */
    public static final String AFTER = Envelope.FieldName.AFTER;

    /**
     * Operation
     */
    public static final String OPERATION = Envelope.FieldName.OPERATION;

    /**
     * CSV
     */
    public static final String CSV = Envelope.FieldName.CSV;

    /**
     * INDEX
     */
    public static final String INDEX = Envelope.FieldName.INDEX;

    /**
     * SLICE
     */
    public static final String SLICE = Envelope.FieldName.SLICES;

    /**
     * SLICESPACE
     */
    public static final String SLICESPACE = Envelope.FieldName.SLICESPACE;

    /**
     * SNAPSHOT
     */
    public static final String SNAPSHOT = Envelope.FieldName.SNAPSHOT;

    private Struct before;
    private Struct after;
    private String operation;
    private String csv;
    private String index;

    private Integer totalSlice;
    private Double spacePerSlice;
    private String snapshot;

    /**
     * Constructor
     *
     * @param value Struct the value
     */
    public DataReplayOperation(Struct value) {
        this.operation = value.getString(OPERATION);
        this.before = value.getStruct(BEFORE);
        this.after = value.getStruct(AFTER);
        this.csv = value.getString(CSV);
        this.index = value.getString(INDEX);
        this.totalSlice = value.getInt32(SLICE);
        this.spacePerSlice = value.getFloat64(SLICESPACE);
        this.snapshot = value.getString(SNAPSHOT);
    }

    /**
     * Gets path
     *
     * @return String the path
     */
    public String getPath() {
        if (csv != null) {
            String[] split = csv.split("\\|");
            return split[0];
        }
        return "";
    }

    /**
     * Gets table total slice number
     *
     * @return Integer the ColumnString
     */
    public Integer getSliceNumber() {
        if (csv != null) {
            String[] split = csv.split("\\|");
            return Integer.parseInt(split[1]);
        }
        return -1;
    }

    public Integer getTotalSlice() {
        return totalSlice;
    }

    public Double getSpacePerSlice() {
        return spacePerSlice;
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

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public String getSnapshot() {
        return snapshot;
    }
}
