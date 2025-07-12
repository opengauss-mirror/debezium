/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Portions Copyright (c) 2025 Huawei Technologies Co.,Ltd.
 * This file has been modified to adapt to openGauss
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
