/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.snapshot;

import java.time.Duration;
import java.util.Optional;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.relational.TableId;

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
    public Optional<String> snapshotTableLockingStatement(Duration lockTimeout, Set<TableId> tableIds) {
        String lineSeparator = System.lineSeparator();
        StringBuilder statements = new StringBuilder();
        statements.append("SET lock_timeout = ").append(lockTimeout.toMillis()).append(";").append(lineSeparator);
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
