/*
 * Copyright 2026 Huawei Technologies Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
