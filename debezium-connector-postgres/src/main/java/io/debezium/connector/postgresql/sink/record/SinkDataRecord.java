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

package io.debezium.connector.postgresql.sink.record;

import io.debezium.connector.postgresql.sink.common.SourceDataField;
import io.debezium.connector.postgresql.sink.object.DataReplayOperation;
import io.debezium.migration.BaseMigrationConfig;

/**
 * Description: SinkRecordObject
 *
 * @author tianbin
 * @since 2024/11/04
 */
public class SinkDataRecord {
    private SourceDataField sourceField;
    private DataReplayOperation dataReplayOperation;
    private Long kafkaOffset;
    private BaseMigrationConfig.MessageType msgType;

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
    public SourceDataField getSourceField() {
        return sourceField;
    }

    /**
     * Sets source field
     *
     * @param sourceField Source the source field
     */
    public void setSourceField(SourceDataField sourceField) {
        this.sourceField = sourceField;
    }

    /**
     * Gets data operation
     *
     * @return DmlOperation the dml operation
     */
    public DataReplayOperation getDataReplayOperation() {
        return dataReplayOperation;
    }

    /**
     * Sets data operation
     *
     * @param dmlOperation DmlOperation the dml operation
     */
    public void setDmlOperation(DataReplayOperation dmlOperation) {
        this.dataReplayOperation = dmlOperation;
    }

    public BaseMigrationConfig.MessageType getMsgType() {
        return msgType;
    }

    public void setMsgType(String msgType) {
        this.msgType = BaseMigrationConfig.MessageType.valueOf(msgType);
    }
}
