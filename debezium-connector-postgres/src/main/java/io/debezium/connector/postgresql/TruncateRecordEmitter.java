/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package io.debezium.connector.postgresql;

import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.ReplicationMessage;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;

/**
 * Description: TruncateRecordEmitter
 *
 * @author jianghongbo
 * @since 2025/02/05
 */
public class TruncateRecordEmitter extends PostgresChangeRecordEmitter {
    /**
     * Constructor
     *
     * @param partition Partition
     * @param offset OffsetContext
     * @param clock Clock
     * @param connectorConfig OpengaussConnectorConfig
     * @param schema OpengaussSchema
     * @param connection OpengaussConnection
     * @param tableId TableId
     * @param message ReplicationMessage
     */
    public TruncateRecordEmitter(Partition partition, OffsetContext offset, Clock clock,
                                 PostgresConnectorConfig connectorConfig, PostgresSchema schema,
                                 PostgresConnection connection, TableId tableId, ReplicationMessage message) {
        super(partition, offset, clock, connectorConfig, schema, connection, tableId, message);
    }
}
