/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.oracle.antlr.OracleDdlParser;
import io.debezium.pipeline.spi.SchemaChangeEventEmitter;
import io.debezium.relational.TableId;
import io.debezium.relational.ddl.DdlChanges;
import io.debezium.relational.ddl.DdlParserListener;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.text.MultipleParsingExceptions;
import io.debezium.text.ParsingException;

/**
 * @author saxisuer
 */
public class OracleIndexChangeEventEmitter implements SchemaChangeEventEmitter {

    private static final Logger LOGGER = LoggerFactory.getLogger(OracleIndexChangeEventEmitter.class);

    private final OraclePartition partition;
    private final OracleOffsetContext offsetContext;
    private final OracleDatabaseSchema schema;
    private final Instant changeTime;
    private final String sourceDatabaseName;
    private final String objectOwner;
    private final String ddlText;
    private TableId tableId;
    private final OracleStreamingChangeEventSourceMetrics streamingMetrics;

    List<SchemaChangeEvent> changeEvents = new ArrayList<>();

    public OracleIndexChangeEventEmitter(OraclePartition partition, OracleOffsetContext offsetContext, String sourceDatabaseName, String objectOwner,
                                         String ddlText, OracleDatabaseSchema schema, Instant changeTime,
                                         OracleStreamingChangeEventSourceMetrics streamingMetrics) {
        this.partition = partition;
        this.offsetContext = offsetContext;
        this.schema = schema;
        this.changeTime = changeTime;
        this.sourceDatabaseName = sourceDatabaseName;
        this.objectOwner = objectOwner;
        this.ddlText = ddlText;
        this.streamingMetrics = streamingMetrics;
        parserSql();
    }

    public void parserSql() {
        final OracleDdlParser parser = schema.getDdlParser();
        final DdlChanges ddlChanges = parser.getDdlChanges();
        try {
            ddlChanges.reset();
            parser.setCurrentDatabase(sourceDatabaseName);
            parser.setCurrentSchema(objectOwner);
            parser.parse(ddlText, schema.getTables());
        }
        catch (ParsingException | MultipleParsingExceptions e) {
            if (schema.skipUnparseableDdlStatements()) {
                LOGGER.warn("Ignoring unparsable DDL statement '{}': {}", ddlText, e);
                streamingMetrics.incrementWarningCount();
                streamingMetrics.incrementUnparsableDdlCount();
            }
            else {
                throw e;
            }
        }

        if (!ddlChanges.isEmpty()) {
            ddlChanges.getEventsByDatabase((String dbName, List<DdlParserListener.Event> events) -> {
                events.forEach(event -> {
                    switch (event.type()) {
                        case CREATE_INDEX:
                            changeEvents.add(createIndexEvent(partition, (DdlParserListener.TableIndexCreatedEvent) event));
                            break;
                        case DROP_INDEX:
                            changeEvents.add(dropIndex(partition, (DdlParserListener.TableIndexDroppedEvent) event));
                            break;
                        default:
                            LOGGER.info("Skipped DDL event type {}: {}", event.type(), ddlText);
                            break;
                    }
                });
            });
        }
    }

    private SchemaChangeEvent dropIndex(OraclePartition partition, DdlParserListener.TableIndexDroppedEvent event) {
        offsetContext.tableEvent(event.tableId(), changeTime);
        if (tableId == null) {
            tableId = event.tableId();
        }
        return new SchemaChangeEvent(partition.getSourcePartition(),
                offsetContext.getOffset(),
                offsetContext.getSourceInfo(),
                event.tableId().catalog(),
                event.tableId().schema(),
                event.statement(),
                schema.tableFor(event.tableId()),
                SchemaChangeEvent.SchemaChangeEventType.DROP_INDEX,
                false);
    }

    @Override
    public void emitSchemaChangeEvent(Receiver receiver) throws InterruptedException {
        LOGGER.info("emitSchemaChangeEvent,changeEventsSize: {}", changeEvents.size());
        for (SchemaChangeEvent event : changeEvents) {
            receiver.schemaChangeEvent(event);
        }
    }

    private SchemaChangeEvent createIndexEvent(OraclePartition partition, DdlParserListener.TableIndexCreatedEvent event) {
        offsetContext.tableEvent(event.tableId(), changeTime);
        if (tableId == null) {
            tableId = event.tableId();
        }
        return new SchemaChangeEvent(partition.getSourcePartition(),
                offsetContext.getOffset(),
                offsetContext.getSourceInfo(),
                event.tableId().catalog(),
                event.tableId().schema(),
                event.statement(),
                schema.tableFor(event.tableId()),
                SchemaChangeEvent.SchemaChangeEventType.CREATE_INDEX,
                false);
    }

    public TableId getTableId() {
        return tableId;
    }
}
