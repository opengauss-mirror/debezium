/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.opengauss.snapshot;

import io.debezium.DebeziumException;
import io.debezium.connector.opengauss.OpengaussConnectorConfig;
import io.debezium.connector.opengauss.spi.OffsetState;
import io.debezium.connector.opengauss.spi.SlotState;
import io.debezium.relational.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Optional;
import java.util.Set;

public class AlwaysSnapshotter extends QueryingSnapshotter {

    private final static Logger LOGGER = LoggerFactory.getLogger(AlwaysSnapshotter.class);

    @Override
    public boolean shouldStream() {
        return true;
    }

    @Override
    public boolean shouldSnapshot() {
        LOGGER.info("Taking a new snapshot as per configuration");
        return true;
    }

    @Override
    public void init(OpengaussConnectorConfig config, OffsetState sourceInfo, SlotState slotState) {
        String exportCsvPath = config.getExportCsvPath();
        if (exportCsvPath == null) {
            throw new DebeziumException("full migration configuration export.csv.path No default value.");
        }
        super.init(config, sourceInfo, slotState);
    }

    /**
     * sql to generate a lock table
     *
     * @param lockTimeout Duration
     * @param tableIds collection of tableId
     * @return sql
     */
    @Override
    public Optional<String> snapshotTableLockingStatement(Duration lockTimeout, Set<TableId> tableIds) {
        String lineSeparator = System.lineSeparator();
        StringBuilder statements = new StringBuilder();
        statements.append("SET lockwait_timeout = ").append(lockTimeout.toMillis()).append(";").append(lineSeparator);
        // Locking can only be done within a transaction, so open the transaction
        statements.append("START TRANSACTION;").append(lineSeparator);
        // Prohibit reading any unexpected operation
        tableIds.forEach(tableId -> statements.append("LOCK TABLE ")
                .append(tableId.toDoubleQuotedString())
                .append(" IN EXCLUSIVE MODE;")
                .append(lineSeparator));
        return Optional.of(statements.toString());
    }
}
