/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.opengauss;

import java.nio.charset.Charset;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import io.debezium.connector.opengauss.process.OgProcessCommitter;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.common.BaseSourceTask;
import io.debezium.connector.opengauss.connection.OpengaussConnection;
import io.debezium.connector.opengauss.connection.OpengaussConnection.PostgresValueConverterBuilder;
import io.debezium.connector.opengauss.connection.OpengaussDefaultValueConverter;
import io.debezium.connector.opengauss.connection.ReplicationConnection;
import io.debezium.connector.opengauss.spi.SlotCreationResult;
import io.debezium.connector.opengauss.spi.SlotState;
import io.debezium.connector.opengauss.spi.Snapshotter;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.metrics.DefaultChangeEventSourceMetricsFactory;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;
import io.debezium.util.Clock;
import io.debezium.util.LoggingContext;
import io.debezium.util.Metronome;
import io.debezium.util.SchemaNameAdjuster;

/**
 * Kafka connect source task which uses Postgres logical decoding over a streaming replication connection to process DB changes.
 *
 * @author Horia Chiorean (hchiorea@redhat.com)
 */
public class OpengaussConnectorTask extends BaseSourceTask<OpengaussPartition, OpengaussOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(OpengaussConnectorTask.class);
    private static final String CONTEXT_NAME = "postgres-connector-task";

    private final ThreadPoolExecutor threadPool = new ThreadPoolExecutor(1, 1, 100,
            TimeUnit.SECONDS, new LinkedBlockingQueue<>(1));

    private volatile OpengaussTaskContext taskContext;
    private volatile ChangeEventQueue<DataChangeEvent> queue;
    private volatile OpengaussConnection jdbcConnection;
    private volatile OpengaussConnection heartbeatConnection;
    private volatile ReplicationConnection replicationConnection = null;
    private volatile OpengaussSchema schema;

    @Override
    public ChangeEventSourceCoordinator<OpengaussPartition, OpengaussOffsetContext> start(Configuration config) {
        final OpengaussConnectorConfig connectorConfig = new OpengaussConnectorConfig(config);
        final TopicSelector<TableId> topicSelector = OpengaussTopicSelector.create(connectorConfig);
        final Snapshotter snapshotter = connectorConfig.getSnapshotter();
        final SchemaNameAdjuster schemaNameAdjuster = SchemaNameAdjuster.create();

        if (connectorConfig.isCommitProcess()) {
            connectorConfig.rectifyParameter();
            statCommit(connectorConfig);
        }

        if (snapshotter == null) {
            throw new ConnectException("Unable to load snapshotter, if using custom snapshot mode, double check your settings");
        }

        heartbeatConnection = new OpengaussConnection(connectorConfig.getJdbcConfig());
        final Charset databaseCharset = heartbeatConnection.getDatabaseCharset();

        final PostgresValueConverterBuilder valueConverterBuilder = (typeRegistry) -> OpengaussValueConverter.of(
                connectorConfig,
                databaseCharset,
                typeRegistry);

        // Global JDBC connection used both for snapshotting and streaming.
        // Must be able to resolve datatypes.
        jdbcConnection = new OpengaussConnection(connectorConfig.getJdbcConfig(), valueConverterBuilder);
        try {
            jdbcConnection.setAutoCommit(false);
        }
        catch (SQLException e) {
            throw new DebeziumException(e);
        }

        final TypeRegistry typeRegistry = jdbcConnection.getTypeRegistry();
        final OpengaussDefaultValueConverter defaultValueConverter = jdbcConnection.getDefaultValueConverter();

        schema = new OpengaussSchema(connectorConfig, typeRegistry, defaultValueConverter, topicSelector, valueConverterBuilder.build(typeRegistry));
        this.taskContext = new OpengaussTaskContext(connectorConfig, schema, topicSelector);
        final Offsets<OpengaussPartition, OpengaussOffsetContext> previousOffsets = getPreviousOffsets(
                new OpengaussPartition.Provider(connectorConfig), new OpengaussOffsetContext.Loader(connectorConfig));
        final Clock clock = Clock.system();
        final OpengaussOffsetContext previousOffset = previousOffsets.getTheOnlyOffset();

        LoggingContext.PreviousContext previousContext = taskContext.configureLoggingContext(CONTEXT_NAME);
        try {
            // Print out the server information
            SlotState slotInfo = null;
            try {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info(jdbcConnection.serverInfo().toString());
                }
                slotInfo = jdbcConnection.getReplicationSlotState(connectorConfig.slotName(), connectorConfig.plugin().getPostgresPluginName());
            }
            catch (SQLException e) {
                LOGGER.warn("unable to load info of replication slot, Debezium will try to create the slot");
            }

            if (previousOffset == null) {
                LOGGER.info("No previous offset found");
                // if we have no initial offset, indicate that to Snapshotter by passing null
                snapshotter.init(connectorConfig, null, slotInfo);
            }
            else {
                LOGGER.info("Found previous offset {}", previousOffset);
                snapshotter.init(connectorConfig, previousOffset.asOffsetState(), slotInfo);
            }

            SlotCreationResult slotCreatedInfo = null;
            if (snapshotter.shouldStream()) {
                final boolean doSnapshot = snapshotter.shouldSnapshot();
                replicationConnection = createReplicationConnection(this.taskContext,
                        doSnapshot, connectorConfig.maxRetries(), connectorConfig.retryDelay());
                // we need to create the slot before we start streaming if it doesn't exist
                // otherwise we can't stream back changes happening while the snapshot is taking place
                if (slotInfo == null) {
                    try {
                        slotCreatedInfo = replicationConnection.createReplicationSlot().orElse(null);
                    }
                    catch (SQLException ex) {
                        String message = "Creation of replication slot failed";
                        if (ex.getMessage().contains("already exists")) {
                            message += "; when setting up multiple connectors for the same database host, please make sure to use a distinct replication slot name for each.";
                        }
                        throw new DebeziumException(message, ex);
                    }
                }
                else {
                    slotCreatedInfo = null;
                }
            }

            try {
                jdbcConnection.commit();
            }
            catch (SQLException e) {
                throw new DebeziumException(e);
            }

            queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                    .pollInterval(connectorConfig.getPollInterval())
                    .maxBatchSize(connectorConfig.getMaxBatchSize())
                    .maxQueueSize(connectorConfig.getMaxQueueSize())
                    .maxQueueSizeInBytes(connectorConfig.getMaxQueueSizeInBytes())
                    .loggingContextSupplier(() -> taskContext.configureLoggingContext(CONTEXT_NAME))
                    .build();

            ErrorHandler errorHandler = new OpengaussErrorHandler(connectorConfig.getLogicalName(), queue);

            final OpengaussEventMetadataProvider metadataProvider = new OpengaussEventMetadataProvider();

            Heartbeat heartbeat = Heartbeat.create(
                    connectorConfig.getHeartbeatInterval(),
                    connectorConfig.getHeartbeatActionQuery(),
                    topicSelector.getHeartbeatTopic(),
                    connectorConfig.getLogicalName(), heartbeatConnection, exception -> {
                        String sqlErrorId = exception.getSQLState();
                        switch (sqlErrorId) {
                            case "57P01":
                                // Postgres error admin_shutdown, see https://www.postgresql.org/docs/12/errcodes-appendix.html
                                throw new DebeziumException("Could not execute heartbeat action query (Error: " + sqlErrorId + ")", exception);
                            case "57P03":
                                // Postgres error cannot_connect_now, see https://www.postgresql.org/docs/12/errcodes-appendix.html
                                throw new RetriableException("Could not execute heartbeat action query (Error: " + sqlErrorId + ")", exception);
                            default:
                                break;
                        }
                    });

            final OpengaussEventDispatcher<TableId> dispatcher = new OpengaussEventDispatcher<>(
                    connectorConfig,
                    topicSelector,
                    schema,
                    queue,
                    connectorConfig.getTableFilters().dataCollectionFilter(),
                    DataChangeEvent::new,
                    OpengaussChangeRecordEmitter::updateSchema,
                    metadataProvider,
                    heartbeat,
                    schemaNameAdjuster,
                    jdbcConnection);

            ChangeEventSourceCoordinator<OpengaussPartition, OpengaussOffsetContext> coordinator = new OpengaussChangeEventSourceCoordinator(
                    previousOffsets,
                    errorHandler,
                    OpengaussConnector.class,
                    connectorConfig,
                    new OpengaussChangeEventSourceFactory(
                            connectorConfig,
                            snapshotter,
                            jdbcConnection,
                            errorHandler,
                            dispatcher,
                            clock,
                            schema,
                            taskContext,
                            replicationConnection,
                            slotCreatedInfo,
                            slotInfo),
                    new DefaultChangeEventSourceMetricsFactory(),
                    dispatcher,
                    schema,
                    snapshotter,
                    slotInfo);

            coordinator.start(taskContext, this.queue, metadataProvider);

            return coordinator;
        }
        finally {
            previousContext.restore();
        }
    }

    public ReplicationConnection createReplicationConnection(OpengaussTaskContext taskContext, boolean doSnapshot, int maxRetries, Duration retryDelay)
            throws ConnectException {
        final Metronome metronome = Metronome.parker(retryDelay, Clock.SYSTEM);
        short retryCount = 0;
        ReplicationConnection replicationConnection = null;
        while (retryCount <= maxRetries) {
            try {
                return taskContext.createReplicationConnection(doSnapshot);
            }
            catch (SQLException ex) {
                retryCount++;
                if (retryCount > maxRetries) {
                    LOGGER.error("Too many errors connecting to server. All {} retries failed.", maxRetries);
                    throw new ConnectException(ex);
                }

                LOGGER.warn("Error connecting to server; will attempt retry {} of {} after {} " +
                        "seconds. Exception message: {}", retryCount, maxRetries, retryDelay.getSeconds(), ex.getMessage());
                try {
                    metronome.pause();
                }
                catch (InterruptedException e) {
                    LOGGER.warn("Connection retry sleep interrupted by exception: " + e);
                    Thread.currentThread().interrupt();
                }
            }
        }
        return replicationConnection;
    }

    @Override
    public List<SourceRecord> doPoll() throws InterruptedException {
        final List<DataChangeEvent> records = queue.poll();
        final List<SourceRecord> sourceRecords = records.stream()
                .map(DataChangeEvent::getRecord)
                .collect(Collectors.toList());
        return sourceRecords;
    }

    @Override
    protected void doStop() {
        // The replication connection is regularly closed at the end of streaming phase
        // in case of error it can happen that the connector is terminated before the stremaing
        // phase is started. It can lead to a leaked connection.
        // This is guard to make sure the connection is closed.
        try {
            if (replicationConnection != null) {
                replicationConnection.close();
            }
        }
        catch (Exception e) {
            LOGGER.trace("Error while closing replication connection", e);
        }

        if (jdbcConnection != null) {
            jdbcConnection.close();
        }

        if (heartbeatConnection != null) {
            heartbeatConnection.close();
        }

        if (schema != null) {
            schema.close();
        }
    }

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    protected Iterable<Field> getAllConfigurationFields() {
        return OpengaussConnectorConfig.ALL_FIELDS;
    }

    public OpengaussTaskContext getTaskContext() {
        return taskContext;
    }

    private void statCommit(OpengaussConnectorConfig connectorConfig) {
        threadPool.execute(() -> {
            OgProcessCommitter processCommitter = new OgProcessCommitter(connectorConfig);
            processCommitter.commitSourceProcessInfo();
        });
    }
}
