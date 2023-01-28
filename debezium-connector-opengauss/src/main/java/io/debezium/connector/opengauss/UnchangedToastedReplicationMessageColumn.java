/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.opengauss;

import io.debezium.connector.opengauss.connection.AbstractReplicationMessageColumn;
import io.debezium.connector.opengauss.connection.ReplicationMessage;
import io.debezium.connector.opengauss.connection.ReplicationStream;

/**
 * Represents a toasted column in a {@link ReplicationStream}.
 *
 * Some decoder implementations may stream information about a column but provide an indicator that the field was not
 * changed and therefore toasted.  This implementation acts as an indicator for such fields that are contained within
 * a {@link ReplicationMessage}.
 *
 * @author Chris Cranford
 */
public class UnchangedToastedReplicationMessageColumn extends AbstractReplicationMessageColumn {

    /**
     * Marker value indicating an unchanged TOAST column value.
     */
    public static final Object UNCHANGED_TOAST_VALUE = new Object();

    public UnchangedToastedReplicationMessageColumn(String columnName, OpengaussType type, String typeWithModifiers, boolean optional, boolean hasMetadata) {
        super(columnName, type, typeWithModifiers, optional, hasMetadata);
    }

    @Override
    public boolean isToastedColumn() {
        return true;
    }

    @Override
    public Object getValue(OpengaussStreamingChangeEventSource.PgConnectionSupplier connection, boolean includeUnknownDatatypes) {
        return UNCHANGED_TOAST_VALUE;
    }
}
