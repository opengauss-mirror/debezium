/**
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.opengauss.sink.object;

/**
 * Description: SinkRecordObject
 *
 * @author wangzhengyuan
 * @since 2022/11/04
 */
public class SinkRecordObject {
    private SourceField sourceField;
    private DmlOperation dmlOperation;
    private Long kafkaOffset;

    /**
     * Get kafka offset
     *
     * @return Long the kafka offset
     */
    public Long getKafkaOffset() {
        return kafkaOffset;
    }

    /**
     * Set kafka offset
     *
     * @param kafkaOffset the record kafka offset
     */
    public void setKafkaOffset(Long kafkaOffset) {
        this.kafkaOffset = kafkaOffset;
    }

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
     * @param sourceField Source the source field
     */
    public void setSourceField(SourceField sourceField) {
        this.sourceField = sourceField;
    }

    /**
     * Gets data operation
     *
     * @return DmlOperation the dml operation
     */
    public DmlOperation getDmlOperation() {
        return dmlOperation;
    }

    /**
     * Sets data operation
     *
     * @param dmlOperation DmlOperation the dml operation
     */
    public void setDmlOperation(DmlOperation dmlOperation) {
        this.dmlOperation = dmlOperation;
    }

}
