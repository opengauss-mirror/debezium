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

package io.debezium.connector.opengauss.connection.ogoutput;

import io.debezium.connector.opengauss.connection.ReplicationMessage;

import java.time.Instant;
import java.util.List;
import java.util.OptionalLong;

/**
 * Replication message instance representing DDL decoding message
 *
 * @author tianbin
 * @since 2024-11-04
 */
public class OgOutputDdlReplicationMessage implements ReplicationMessage {
    private final Operation operation;
    private final Instant commitTime;
    private final Long transactionId;
    private final String prefix;
    private final String body;

    public OgOutputDdlReplicationMessage(ReplicationMessage.Operation op, Instant commitTimestamp, Long transactionId,
                                         String prefix, String body) {
        this.operation = op;
        this.commitTime = commitTimestamp;
        this.transactionId = transactionId;
        this.prefix = prefix;
        this.body = body;
    }

    @Override
    public boolean isLastEventForLsn() {
        return true;
    }

    @Override
    public Operation getOperation() {
        return operation;
    }

    @Override
    public Instant getCommitTime() {
        return commitTime;
    }

    @Override
    public OptionalLong getTransactionId() {
        return transactionId == null ? OptionalLong.empty() : OptionalLong.of(transactionId);
    }

    @Override
    public String getTable() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Column> getOldTupleList() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Column> getNewTupleList() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasTypeMetadata() {
        throw new UnsupportedOperationException();
    }

    public String getPrefix() {
        return prefix;
    }

    public String getBody() {
        return body;
    }
}
