/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.opengauss.snapshot.metadata;

import io.debezium.config.Field;
import io.debezium.connector.opengauss.Module;
import io.debezium.connector.opengauss.OpengaussConnector;
import io.debezium.connector.opengauss.OpengaussConnectorConfig;
import io.debezium.metadata.ConnectorDescriptor;
import io.debezium.metadata.ConnectorMetadata;

public class OpengaussConnectorMetadata implements ConnectorMetadata {

    @Override
    public ConnectorDescriptor getConnectorDescriptor() {
        return new ConnectorDescriptor("postgres", "Debezium PostgreSQL Connector", OpengaussConnector.class.getName(), Module.version());
    }

    @Override
    public Field.Set getConnectorFields() {
        return OpengaussConnectorConfig.ALL_FIELDS;
    }

}
