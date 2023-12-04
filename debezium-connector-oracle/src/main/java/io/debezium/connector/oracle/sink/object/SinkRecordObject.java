/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.sink.object;

/**
 * Description: SinkRecordObject
 * @author gbase
 * @date 2023/07/28
 **/
public class SinkRecordObject {
    private SourceField sourceField;
    private DataOperation dataOperation;

    /**
     * Gets source field
     *
     * @return SourceField the source field
     */
    public SourceField getSourceField() {
        return sourceField;
    }

    /**
     * Sets source field
     *
     * @param sourceField the source field
     */
    public void setSourceField(SourceField sourceField) {
        this.sourceField = sourceField;
    }

    /**
     * Gets data operation
     *
     * @return DataOperation the data operation
     */
    public DataOperation getDataOperation() {
        return dataOperation;
    }

    /**
     * Sets data operation
     *
     * @param dataOperation the data operation
     */
    public void setDataOperation(DataOperation dataOperation) {
        this.dataOperation = dataOperation;
    }

    @Override
    public String toString() {
        return "SinkRecordObject{" +
                "sourceField=" + sourceField +
                ", dataOperation=" + dataOperation +
                '}';
    }
}
