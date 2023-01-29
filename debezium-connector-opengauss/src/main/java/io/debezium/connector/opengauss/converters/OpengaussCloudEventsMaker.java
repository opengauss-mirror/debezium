/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.opengauss.converters;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.converters.spi.CloudEventsMaker;
import io.debezium.converters.spi.RecordParser;
import io.debezium.converters.spi.SerializerType;

/**
 * CloudEvents maker for records producer by the PostgreSQL connector.
 *
 * @author Chris Cranford
 */
public class OpengaussCloudEventsMaker extends CloudEventsMaker {

    public OpengaussCloudEventsMaker(RecordParser parser, SerializerType contentType, String dataSchemaUriBase) {
        super(parser, contentType, dataSchemaUriBase);
    }

    @Override
    public String ceId() {
        return "name:" + recordParser.getMetadata(AbstractSourceInfo.SERVER_NAME_KEY)
                + ";lsn:" + recordParser.getMetadata(OpengaussRecordParser.LSN_KEY).toString()
                + ";txId:" + recordParser.getMetadata(OpengaussRecordParser.TXID_KEY).toString();
    }
}
