/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.sink.object;

/**
 * Description: SinkRecordObject
 * @author douxin
 * @since 2022/10/31
 **/
public class SinkRecordObject {
    private SourceField sourceField;
    private DataOperation dataOperation;
    private long kafkaOffset;

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
     * @param SourceField the source field
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
     * @param DataOperation the data operation
     */
    public void setDataOperation(DataOperation dataOperation) {
        this.dataOperation = dataOperation;
    }

    /**
     * Gets kafkaOffset.
     *
     * @return the value of kafkaOffset
     */
    public long getKafkaOffset() {
        return kafkaOffset;
    }

    /**
     * Sets the kafkaOffset.
     *
     * @param kafkaOffset offset stored in Kafka records
     */
    public void setKafkaOffset(long kafkaOffset) {
        this.kafkaOffset = kafkaOffset;
    }

    @Override
    public String toString() {
        return "SinkRecordObject{"
                + "sourceField=" + sourceField
                + ", dataOperation=" + dataOperation
                + ", kafkaOffset=" + kafkaOffset
                + '}';
    }
}
