/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.opengauss.connection;

import io.debezium.connector.opengauss.OpengaussConnectorConfig;
import io.debezium.connector.opengauss.OpengaussSchema;

/**
 * Contextual data required by {@link MessageDecoder}s.
 *
 * @author Chris Cranford
 */
public class MessageDecoderContext {

    private final OpengaussConnectorConfig config;
    private final OpengaussSchema schema;

    public MessageDecoderContext(OpengaussConnectorConfig config, OpengaussSchema schema) {
        this.config = config;
        this.schema = schema;
    }

    public OpengaussConnectorConfig getConfig() {
        return config;
    }

    public OpengaussSchema getSchema() {
        return schema;
    }
}
