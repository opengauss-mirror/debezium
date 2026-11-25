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

/**
 * Description: SinkRecordObject
 *
 * @author tianbin
 * @since 2024-11-25
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
