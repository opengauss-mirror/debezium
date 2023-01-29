/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.opengauss.junit;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import io.debezium.connector.opengauss.TestHelper;
import io.debezium.connector.opengauss.connection.OpengaussConnection;
import io.debezium.junit.DatabaseVersionResolver;

/**
 * Implementation of {@link DatabaseVersionResolver} specific for PostgreSQL.
 *
 * @author Chris Cranford
 */
public class OpengaussDatabaseVersionResolver implements DatabaseVersionResolver {
    @Override
    public DatabaseVersion getVersion() {
        try {
            try (final OpengaussConnection postgresConnection = TestHelper.create()) {
                final DatabaseMetaData metadata = postgresConnection.connection().getMetaData();
                return new DatabaseVersion(metadata.getDatabaseMajorVersion(), metadata.getDatabaseMinorVersion(), 0);
            }
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
