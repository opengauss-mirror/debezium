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
