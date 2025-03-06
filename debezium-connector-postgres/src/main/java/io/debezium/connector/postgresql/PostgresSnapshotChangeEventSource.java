/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static io.debezium.util.Strings.isNumeric;

import java.io.File;
import java.io.IOException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Statement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.util.HashMap;
import java.util.Set;
import java.util.Map;
import java.util.List;
import java.util.Optional;
import java.util.HashSet;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.OptionalLong;
import java.util.Locale;
import java.util.StringJoiner;
import java.util.Collections;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import io.debezium.connector.postgresql.param.PostgresDataEventsParam;
import io.debezium.connector.postgresql.param.PostgresPageSliceParam;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.connect.data.Struct;
import org.postgresql.core.ServerVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.postgresql.connection.Lsn;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.ReplicationMessage;
import io.debezium.connector.postgresql.migration.MigrationUtil;
import io.debezium.connector.postgresql.migration.PostgresObjectDdlFactory;
import io.debezium.connector.postgresql.migration.PostgresSqlConstant;
import io.debezium.connector.postgresql.migration.partition.HashPartitionInfo;
import io.debezium.connector.postgresql.migration.partition.ListPartitionInfo;
import io.debezium.connector.postgresql.migration.partition.PartitionInfo;
import io.debezium.connector.postgresql.migration.partition.RangePartitionInfo;
import io.debezium.connector.postgresql.process.PgProcessCommitter;
import io.debezium.connector.postgresql.sink.utils.TimeUtil;
import io.debezium.connector.postgresql.spi.SlotCreationResult;
import io.debezium.connector.postgresql.spi.SlotState;
import io.debezium.connector.postgresql.spi.Snapshotter;
import io.debezium.migration.ObjectChangeEvent;
import io.debezium.migration.ObjectEnum;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.relational.Table;
import io.debezium.relational.Column;
import io.debezium.relational.TableSchema;
import io.debezium.relational.TableId;
import io.debezium.relational.RelationalSnapshotChangeEventSource;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.schema.SchemaChangeEvent.SchemaChangeEventType;
import io.debezium.util.Clock;
import io.debezium.util.ColumnUtils;
import io.debezium.util.HexConverter;
import io.debezium.util.Threads;

/**
 * Description: PostgresSnapshotChangeEventSource
 *
 * @author jianghongbo
 * @since 2025/02/05
 */
public class PostgresSnapshotChangeEventSource extends
        RelationalSnapshotChangeEventSource<PostgresPartition, PostgresOffsetContext> {
    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresSnapshotChangeEventSource.class);
    private static final int MEMORY_UNIT = 1024;
    private static final String DELIMITER = ",";
    private static final String LINESEP = System.lineSeparator();
    private static final String LIMIT_STATEMENT = " limit %s offset %s ";
    private static final Integer MAX_EXPORT_PAGE_NUMBER = 100000;
    private static final Integer EMPTY_TABLE_PAGEROWS = 100000;

    private final PostgresConnectorConfig connectorConfig;
    private final PostgresConnection jdbcConnection;
    private final PostgresSchema schema;
    private final Snapshotter snapshotter;
    private final SlotCreationResult slotCreatedInfo;
    private final SlotState startingSlotInfo;
    private final Object messLock = new Object();
    private final Object dirLock = new Object();
    private final Object lsnLock = new Object();

    private Map<String, Set<String>> migrationSchemaTables;
    private String csvPath;
    private BigInteger csvDirSize;
    private BigInteger pageSize = BigInteger.valueOf(2 * MEMORY_UNIT * MEMORY_UNIT);
    private AtomicInteger unlockCount = new AtomicInteger(0);
    private BlockingQueue<PostgresTableIndexTask> indexTaskQueue = new LinkedBlockingQueue();
    private Integer workers = 8;
    private PgProcessCommitter pgProcessCommitter;
    private String serverVersion;
    private int exportPageNumber;

    public PostgresSnapshotChangeEventSource(PostgresConnectorConfig connectorConfig, Snapshotter snapshotter,
                                             PostgresConnection jdbcConnection, PostgresSchema schema,
                                             EventDispatcher<TableId> dispatcher, Clock clock,
                                             SnapshotProgressListener snapshotProgressListener,
                                             SlotCreationResult slotCreatedInfo, SlotState startingSlotInfo,
                                             PostgresObjectDdlFactory postgresObjectDdlFactory) {
        super(connectorConfig, jdbcConnection, schema, dispatcher,
                clock, snapshotProgressListener, postgresObjectDdlFactory);
        this.connectorConfig = connectorConfig;
        this.jdbcConnection = jdbcConnection;
        this.schema = schema;
        this.snapshotter = snapshotter;
        this.slotCreatedInfo = slotCreatedInfo;
        this.startingSlotInfo = startingSlotInfo;
        this.migrationSchemaTables = new HashMap<>();
        this.csvPath = connectorConfig.getExportCsvPath();
    }

    @Override
    protected SnapshottingTask getSnapshottingTask(PostgresOffsetContext previousOffset) {
        boolean snapshotSchema = true;
        boolean snapshotData = true;

        snapshotData = snapshotter.shouldSnapshot();
        if (snapshotData) {
            LOGGER.info("According to the connector configuration data will be snapshotted");
        }
        else {
            LOGGER.info("According to the connector configuration no snapshot will be executed");
            snapshotSchema = false;
        }

        return new SnapshottingTask(snapshotSchema, snapshotData);
    }

    @Override
    protected SnapshotContext<PostgresPartition, PostgresOffsetContext> prepare(PostgresPartition partition)
            throws Exception {
        return new PostgresSnapshotContext(partition, connectorConfig.databaseName());
    }

    @Override
    protected void connectionCreated(RelationalSnapshotContext<PostgresPartition, PostgresOffsetContext> snapshotContext)
            throws Exception {
    }

    /**
     * get server version from pg server
     *
     * @param conn Connection
     * @return String
     * @throws SQLException sqlexception
     */
    protected String getServerVersion(Connection conn) throws SQLException {
        String pgServerVersion = "";
        try (Statement stmt = conn.createStatement();
                ResultSet rst = stmt.executeQuery(PostgresSqlConstant.PG_SHOW_SERVER_VERSION)) {
            if (rst.next()) {
                pgServerVersion = rst.getString(1);
            }
        }
        return pgServerVersion;
    }

    /**
     * set server version
     *
     * @param serverVersion String
     */
    public void setServerVersion(String serverVersion) {
        this.serverVersion = serverVersion;
    }

    @Override
    protected void determineCapturedTables(RelationalSnapshotContext<PostgresPartition, PostgresOffsetContext> ctx,
                                           Connection conn) throws Exception {
        setServerVersion(getServerVersion(conn));
        Set<TableId> allTableIds = determineDataCollectionsToBeSnapshotted(getAllTableIds(ctx))
                .collect(Collectors.toSet());
        Set<TableId> allTableIdsExcludeSystemSchema = filterSystemSchemaTable(allTableIds);

        Set<TableId> capturedTables = new HashSet<>();

        for (TableId tableId : allTableIdsExcludeSystemSchema) {
            String schemaName = tableId.schema();
            String tableName = tableId.table();
            if (connectorConfig.getTableFilters().dataCollectionFilter().isIncluded(tableId)
                    && !isPartitionTable(schemaName, tableName, conn)) {
                LOGGER.trace("Adding table {} to the list of captured tables", tableId);
                capturedTables.add(tableId);
            } else {
                LOGGER.trace("Ignoring table {} as it's not included in the filter configuration", tableId);
            }
        }

        ctx.capturedTables = capturedTables;
    }

    private int getServerVersionNum(String versionStr) {
        return ServerVersion.from(versionStr).getVersionNum();
    }

    private boolean isPartitionTable(String schema, String table, Connection connection) {
        // below pg10 no exist partition table
        if (getServerVersionNum(serverVersion) < getServerVersionNum(PostgresSqlConstant.PG_SERVER_V10)) {
            return false;
        }
        boolean hasPartition = false;
        try (Statement stmt = connection.createStatement();
             ResultSet rst = stmt.executeQuery(String.format(PostgresSqlConstant.HAVE_PARTITION_SQL, schema, table))) {
            if (rst.next()) {
                hasPartition = rst.getBoolean(1);
            } else {
                LOGGER.error("cannot get partition info of table {}.{}", schema, table);
            }
        } catch (SQLException e) {
            LOGGER.error("get partition info of table {}.{} occurred SQLException", schema, table, e);
        }
        return hasPartition;
    }

    @Override
    protected List<String> determineCapturedSchemas(Connection conn) {
        List<String> allSchemas = getAllSchema(conn);
        List<String> schemaWhiteList = connectorConfig.schemaWhiteList();
        List<String> capturedSchemas = null;
        if (schemaWhiteList.isEmpty()) {
            capturedSchemas = allSchemas.stream().collect(Collectors.toList());
        } else {
            capturedSchemas = schemaWhiteList.stream().map(String::trim).collect(Collectors.toList());
        }
        return capturedSchemas;
    }

    private Map<String, Set<String>> convertTableWhiteList(List<String> tableWhiteList) {
        Map<String, Set<String>> tableWhiteListMap = new HashMap<>();
        for (String fullTablename : tableWhiteList) {
            String[] schemaTableArr = fullTablename.trim().split("\\.");
            if (schemaTableArr.length != 2) {
                throw new IllegalArgumentException("tableWhiteList name is not correct, should be schema.table");
            }
            String schemaName = schemaTableArr[0];
            String tableName = schemaTableArr[1];
            if (tableWhiteListMap.containsKey(schemaName)) {
                tableWhiteListMap.get(schemaName).add(tableName);
            } else {
                tableWhiteListMap.put(schemaName, new HashSet<>(Arrays.asList(tableName)));
            }
        }
        return tableWhiteListMap;
    }

    private List<String> getAllSchema(Connection conn) {
        List<String> schemaList = new ArrayList<>();
        try (Statement stmt = conn.createStatement();
                ResultSet rst = stmt.executeQuery(PostgresSqlConstant.GETALLSCHEMA)) {
            while (rst.next()) {
                schemaList.add(rst.getString("schema_name"));
            }
        } catch (SQLException e) {
            LOGGER.error("get all schema from database failed", e);
        }
        return schemaList;
    }

    @Override
    protected Set<TableId> getAllTableIds(RelationalSnapshotContext<PostgresPartition, PostgresOffsetContext> ctx)
            throws Exception {
        return jdbcConnection.readTableNames(ctx.catalogName, null, null, new String[]{ "TABLE" });
    }

    @Override
    protected void lockTablesForSchemaSnapshot(ChangeEventSourceContext sourceContext,
                                               RelationalSnapshotContext<PostgresPartition, PostgresOffsetContext> snapshotContext)
            throws SQLException, InterruptedException {

    }

    private void readAndSendXlogLocation(Statement statement, PostgresDataEventsParam dataEventsParam)
            throws SQLException, InterruptedException {
        TableId tableId = dataEventsParam.getTableId();
        LOGGER.info("start get and write xloglocation for table {}.{}", tableId.schema(), tableId.table());
        String xlogLocation = "";
        String getSnapshotStmt =
                getServerVersionNum(serverVersion) < getServerVersionNum(PostgresSqlConstant.PG_SERVER_V10)
                        ? PostgresSqlConstant.GET_XLOG_LOCATION_OLD : PostgresSqlConstant.GET_XLOG_LOCATION_NEW;
        try (ResultSet rst = statement.executeQuery(getSnapshotStmt)) {
            if (rst.next()) {
                xlogLocation = rst.getString(1);
            }
        }

        RelationalSnapshotContext<PostgresPartition, PostgresOffsetContext> snapshotContext
                = dataEventsParam.getSnapshotContext();
        EventDispatcher.SnapshotReceiver snapshotReceiver = dataEventsParam.getSnapshotReceiver();
        synchronized (lsnLock) {
            snapshotContext.offset.event(tableId, getClock().currentTime());
            ChangeRecordEmitter xlogEmitter = new PostgresXlogLocationEmitter(snapshotContext.partition,
                    snapshotContext.offset, getClock(), xlogLocation);
            dispatcher.dispatchSnapshotEvent(tableId, xlogEmitter, snapshotReceiver);
        }
    }

    @Override
    protected void releaseSchemaSnapshotLocks(RelationalSnapshotContext<PostgresPartition, PostgresOffsetContext> snapshotContext)
            throws SQLException {
    }

    @Override
    protected void releaseDataSnapshotLocks(
            RelationalSnapshotContext<PostgresPartition, PostgresOffsetContext> snapshotContext) throws Exception {
        jdbcConnection.executeWithoutCommitting("ROLLBACK;");
    }

    @Override
    protected void determineSnapshotOffset(RelationalSnapshotContext<PostgresPartition, PostgresOffsetContext> ctx, PostgresOffsetContext previousOffset)
            throws Exception {
        PostgresOffsetContext offset = ctx.offset;
        if (offset == null) {
            if (previousOffset != null && !snapshotter.shouldStreamEventsStartingFromSnapshot()) {
                // The connect framework, not the connector, manages triggering committing offset state so the
                // replication stream may not have flushed the latest offset state during catch up streaming.
                // The previousOffset variable is shared between the catch up streaming and snapshot phases and
                // has the latest known offset state.
                offset = PostgresOffsetContext.initialContext(connectorConfig, jdbcConnection, getClock(),
                        previousOffset.lastCommitLsn(), previousOffset.lastCompletelyProcessedLsn());
            }
            else {
                offset = PostgresOffsetContext.initialContext(connectorConfig, jdbcConnection, getClock());
            }
            ctx.offset = offset;
        }

        updateOffsetForSnapshot(offset);
    }

    @Override
    protected void executeObjectSnapShot(
            RelationalSnapshotContext<PostgresPartition, PostgresOffsetContext> snapshotContext,
            Connection connection) {
        Map<ObjectEnum, Map<String, String>> objectDdls = new HashMap<>();
        try {
            List<String> schemas = determineCapturedSchemas(connection);
            for (String schema : schemas) {
                LOGGER.info("start extract object from schema {}", schema);
                switchSchema(schema, connection);
                objectDdls.put(ObjectEnum.VIEW, addViewDdls(schema, connection));
                objectDdls.put(ObjectEnum.FUNCTION, addFuncDdls(schema, connection));
                objectDdls.put(ObjectEnum.TRIGGER, addTriggerDdls(schema, connection));
                dispatchObjectInfo(snapshotContext, schema, objectDdls);
                LOGGER.info("finish extract object from schema {}", schema);
            }
        } catch (InterruptedException e) {
            LOGGER.error("executeObjectSnapShot occurred Exception ", e);
        }
    }

    private void dispatchObjectInfo(RelationalSnapshotContext<PostgresPartition, PostgresOffsetContext> snapshotContext,
                                   String schema,
                                   Map<ObjectEnum, Map<String, String>> objectDdls)
            throws InterruptedException {
        ObjectEnum[] objectOrder = {ObjectEnum.VIEW, ObjectEnum.FUNCTION, ObjectEnum.TRIGGER};
        for (ObjectEnum objType : objectOrder) {
            Map<String, String> objectInfo = objectDdls.get(objType);
            if (objectInfo == null) {
                continue;
            }
            for (Map.Entry<String, String> infoEntry : objectInfo.entrySet()) {
                String objName = infoEntry.getKey();
                String ddl = infoEntry.getValue();
                LOGGER.info("object ddl: {}", ddl);
                dispatcher.dispatchObjectChangeEvent((receiver -> {
                    try {
                        receiver.objectChangeEvent(
                                getCreateObjectEvent(snapshotContext, schema, ddl, objName, objType));
                    } catch (InterruptedException | SQLException e) {
                        LOGGER.error("dispatch object to kafka occurred Exception", e);
                        throw new DebeziumException(e);
                    }
                }));
            }
        }
    }

    private void updateOffsetForSnapshot(PostgresOffsetContext offset) throws SQLException {
        final Lsn xlogStart = getTransactionStartLsn();
        final long txId = jdbcConnection.currentTransactionId().longValue();
        LOGGER.info("Read xlogStart at '{}' from transaction '{}'", xlogStart, txId);

        // use the old xmin, as we don't want to update it if in xmin recovery
        offset.updateWalPosition(xlogStart, offset.lastCompletelyProcessedLsn(), clock.currentTime(), txId, offset.xmin(), null);
    }

    protected void updateOffsetForPreSnapshotCatchUpStreaming(PostgresOffsetContext offset) throws SQLException {
        updateOffsetForSnapshot(offset);
        offset.setStreamingStoppingLsn(Lsn.valueOf(jdbcConnection.currentXLogLocation()));
    }

    private Lsn getTransactionStartLsn() throws SQLException {
        if (slotCreatedInfo != null) {
            // When performing an exported snapshot based on a newly created replication slot, the txLogStart position
            // should be based on the replication slot snapshot transaction point. This is crucial so that if any
            // SQL operations occur mid-snapshot that they'll be properly captured when streaming begins; otherwise
            // they'll be lost.
            return slotCreatedInfo.startLsn();
        }
        else if (!snapshotter.shouldStreamEventsStartingFromSnapshot() && startingSlotInfo != null) {
            // Allow streaming to resume from where streaming stopped last rather than where the current snapshot starts.
            SlotState currentSlotState = jdbcConnection.getReplicationSlotState(connectorConfig.slotName(),
                    connectorConfig.plugin().getPostgresPluginName());
            return currentSlotState.slotLastFlushedLsn();
        }

        return Lsn.valueOf(jdbcConnection.currentXLogLocation());
    }

    @Override
    protected void readTableStructure(ChangeEventSourceContext sourceContext,
                                      RelationalSnapshotContext<PostgresPartition, PostgresOffsetContext> snapshotContext,
                                      PostgresOffsetContext offsetContext)
            throws SQLException, InterruptedException {
        Set<String> schemas = snapshotContext.capturedTables.stream()
                .map(TableId::schema)
                .collect(Collectors.toSet());

        // reading info only for the schemas we're interested in as per the set of captured tables;
        // while the passed table name filter alone would skip all non-included tables, reading the schema
        // would take much longer that way
        for (String schema : schemas) {
            if (!sourceContext.isRunning()) {
                throw new InterruptedException("Interrupted while reading structure of schema " + schema);
            }

            LOGGER.info("Reading structure of schema '{}'", snapshotContext.catalogName);
            jdbcConnection.readSchema(
                    snapshotContext.tables,
                    snapshotContext.catalogName,
                    schema,
                    null,
                    null,
                    false);
        }
        schema.refresh(jdbcConnection, false);
    }

    /**
     * @description: get create table event
     *
     * @param snapshotContext RelationalSnapshotContext<PostgresPartition, PostgresOffsetContext>
     * @param table Table
     * @return SchemaChangeEvent
     */
    @Override
    protected SchemaChangeEvent getCreateTableEvent(
            RelationalSnapshotContext<PostgresPartition, PostgresOffsetContext> snapshotContext, Table table)
            throws SQLException {
        return new SchemaChangeEvent(
                snapshotContext.partition.getSourcePartition(),
                snapshotContext.offset.getOffset(),
                snapshotContext.offset.getSourceInfo(),
                snapshotContext.catalogName,
                table.id().schema(),
                null,
                table,
                SchemaChangeEventType.CREATE,
                true);
    }

    @Override
    protected SchemaChangeEvent getCreateTableEvent(RelationalSnapshotContext<PostgresPartition, PostgresOffsetContext> snapshotContext,
                                                    Table table, String parentTables, String partititonDdl) {
        return new SchemaChangeEvent(
                snapshotContext.partition.getSourcePartition(),
                snapshotContext.offset.getOffset(),
                snapshotContext.offset.getSourceInfo(),
                snapshotContext.catalogName,
                table.id().schema(),
                null,
                table,
                SchemaChangeEventType.CREATE,
                true, parentTables, partititonDdl);
    }

    @Override
    protected ObjectChangeEvent getCreateObjectEvent(
            RelationalSnapshotContext<PostgresPartition, PostgresOffsetContext> snapshotContext,
            String schema, String ddl, String objName, ObjectEnum objType) throws SQLException {
        return new ObjectChangeEvent(snapshotContext.partition.getSourcePartition(),
                snapshotContext.offset.getOffset(),
                snapshotContext.offset.getSourceInfo(),
                snapshotContext.catalogName,
                schema,
                ddl,
                objName,
                objType);
    }

    @Override
    protected void complete(SnapshotContext<PostgresPartition, PostgresOffsetContext> snapshotContext) {
        snapshotter.snapshotCompleted();
    }

    /**
     * Generate a valid Postgres query string for the specified table and columns
     *
     * @param tableId the table to generate a query for
     * @return a valid query string
     */
    @Override
    protected Optional<String> getSnapshotSelect(RelationalSnapshotContext<PostgresPartition, PostgresOffsetContext> snapshotContext,
                                                 TableId tableId, List<String> columns) {
        return snapshotter.buildSnapshotQuery(tableId, columns);
    }

    protected void setSnapshotTransactionIsolationLevel() throws SQLException {
        LOGGER.info("Setting isolation level");
        String transactionStatement = snapshotter.snapshotTransactionIsolationLevelStatement(slotCreatedInfo);
        LOGGER.info("Opening transaction with statement {}", transactionStatement);
        jdbcConnection.executeWithoutCommitting(transactionStatement);
    }

    /**
     * Mutable context which is populated in the course of snapshotting.
     */
    private static class PostgresSnapshotContext extends RelationalSnapshotContext<PostgresPartition, PostgresOffsetContext> {
        public PostgresSnapshotContext(PostgresPartition partition, String catalogName) throws SQLException {
            super(partition, catalogName);
        }
    }

    /**
     * create schema change events for tables
     *
     * @param sourceContext ChangeEventSourceContext
     * @param snapshotContext RelationalSnapshotContext<PostgresPartition, PostgresOffsetContext>
     * @param snapshottingTask SnapshottingTask
     * @param connection Connection
     */
    protected void createSchemaChangeEventsForTables(
            ChangeEventSourceContext sourceContext,
            RelationalSnapshotContext<PostgresPartition, PostgresOffsetContext> snapshotContext,
            SnapshottingTask snapshottingTask, Connection connection) throws Exception {
        tryStartingSnapshot(snapshotContext);
        for (Iterator<TableId> iterator = snapshotContext.capturedTables.iterator(); iterator.hasNext();) {
            final TableId tableId = iterator.next();
            if (!sourceContext.isRunning()) {
                throw new InterruptedException("Interrupted while capturing schema of table " + tableId);
            }

            LOGGER.debug("Capturing structure of table {}", tableId);

            Table table = snapshotContext.tables.forTable(tableId);
            String parentTables = getParentTables(table, connection);
            String partitionDdl = getPartitionDdl(table, connection);

            snapshotContext.offset.event(tableId, getClock().currentTime());

            // If data are not snapshotted then the last schema change must set last snapshot flag
            if (!snapshottingTask.snapshotData() && !iterator.hasNext()) {
                lastSnapshotRecord(snapshotContext);
            }

            dispatcher.dispatchSchemaChangeEvent(table.id(), (receiver) -> {
                try {
                    receiver.schemaChangeEvent(getCreateTableEvent(snapshotContext, table, parentTables, partitionDdl));
                } catch (InterruptedException e) {
                    throw new DebeziumException(e);
                }
            });
        }
    }

    @Override
    protected void createDataEvents(ChangeEventSourceContext sourceContext,
                                    RelationalSnapshotContext<PostgresPartition, PostgresOffsetContext> snapshotContext)
            throws Exception {
        initVariables();
        EventDispatcher.SnapshotReceiver receiver = dispatcher.getFullSnapshotChangeEventReceiver();
        List<String> schemaList = appointSchemas();
        pushTruncateMessageForTable(snapshotContext, receiver, schemaList);
        tryStartingSnapshot(snapshotContext);
        Set<TableId> tableIds = snapshotContext.capturedTables.stream().filter(o -> schemaList.contains(o.schema()))
                .collect(Collectors.toSet());
        final int tableCount = tableIds.size();
        AtomicInteger tableOrder = new AtomicInteger(1);
        LOGGER.info("Snapshotting contents of {} tables while still in transaction", tableCount);
        // init poolExecutor
        workers = Math.min(tableCount,
                connectorConfig.migrationWorkers() > 0 ? connectorConfig.migrationWorkers() : 1);
        ThreadPoolExecutor poolExecutor = new ThreadPoolExecutor(workers, workers,
                5, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
        List<String> tableNameList = null;
        if (connectorConfig.isCommitProcess()) {
            pgProcessCommitter = new PgProcessCommitter(connectorConfig, PgProcessCommitter.FULL_PROCESS_SUFFIX);
            tableNameList = new ArrayList<>();
        }
        for (Iterator<TableId> tableIdIterator = tableIds.iterator(); tableIdIterator.hasNext();) {
            final TableId tableId = tableIdIterator.next();
            boolean isLastTable = !tableIdIterator.hasNext();

            if (!sourceContext.isRunning()) {
                throw new InterruptedException("Interrupted while snapshotting table " + tableId);
            }

            LOGGER.debug("Snapshotting table {}", tableId);
            poolExecutor.execute(() -> {
                PostgresDataEventsParam dataEventsParam = new PostgresDataEventsParam(sourceContext, snapshotContext,
                        receiver, tableId, isLastTable);
                createDataEventsForTable(dataEventsParam, tableOrder.getAndIncrement(), tableCount);
            });
            if (connectorConfig.isCommitProcess()) {
                tableNameList.add(tableId.schema() + "." + tableId.table());
            }
        }
        if (connectorConfig.isCommitProcess()) {
            pgProcessCommitter.commitSourceTableList(tableNameList);
        }
        // Wait for data collection to complete
        while (poolExecutor.getTaskCount() != poolExecutor.getCompletedTaskCount()) {
            Thread.sleep(1000);
        }
        poolExecutor.shutdown();
        lastSnapshotRecord(snapshotContext);
        while (!indexTaskQueue.isEmpty()) {
            dispatchIndexTask(receiver);
        }
        snapshotContext.offset.preSnapshotCompletion();
        receiver.completeSnapshot();
        snapshotContext.offset.postSnapshotCompletion();
    }

    private void initVariables() throws IOException {
        if (!new File(csvPath).exists()) {
            Files.createDirectories(Paths.get(csvPath));
        }
        if (connectorConfig.getExportCsvPathSize() != null && !connectorConfig.getExportCsvPathSize().isEmpty()) {
            csvDirSize = initCsvDirSize();
        }
        if (connectorConfig.getExportFileSize() != null && !connectorConfig.getExportFileSize().isEmpty()) {
            pageSize = initPagePartitionSize();
        }
        exportPageNumber = connectorConfig.exportPageNumber();
        if (exportPageNumber > MAX_EXPORT_PAGE_NUMBER || exportPageNumber <= 0) {
            throw new IllegalArgumentException("export.page.number must be (0, 100000]");
        }
    }

    private void dispatchIndexTask(EventDispatcher.SnapshotReceiver receiver) throws InterruptedException {
        PostgresTableIndexTask indexTask = indexTaskQueue.take();
        PostgresDataEventsParam postgresDataEventsParam = indexTask.getPostgresDataEventsParam();
        TableId oldtableId = postgresDataEventsParam.getTableId();
        for (String indexDdl : indexTask.getTableIdxDdls()) {
            LOGGER.info("indexDdl: {}", indexDdl);
            synchronized (messLock) {
                dispatcher.dispatchSnapshotEvent(oldtableId,
                        getIndexRecordEmitter(postgresDataEventsParam.getSnapshotContext(), oldtableId, indexDdl),
                        receiver);
            }
        }
    }

    private BigInteger initCsvDirSize() {
        String csvPathSize = connectorConfig.getExportCsvPathSize();
        LOGGER.info("config: export.csv.path.size = {}", csvPathSize);
        if (isNumeric(csvPathSize)) {
            int size = Integer.parseInt(csvPathSize);
            BigInteger unit = BigInteger.valueOf(MEMORY_UNIT);
            return BigInteger.valueOf(size).multiply(unit).multiply(unit).multiply(unit);
        }
        return initSizeOfConfig(csvPathSize, csvDirSize);
    }

    private BigInteger initPagePartitionSize() {
        String exportFileSize = connectorConfig.getExportFileSize();
        LOGGER.info("config: export.file.size = {}", exportFileSize);
        if (isNumeric(exportFileSize)) {
            int size = Integer.parseInt(exportFileSize);
            BigInteger unit = BigInteger.valueOf(MEMORY_UNIT);
            return BigInteger.valueOf(size).multiply(unit).multiply(unit);
        }
        return initSizeOfConfig(exportFileSize, pageSize);
    }

    private List<String> appointSchemas() throws SQLException {
        List<String> schemaList = new ArrayList<>();
        try (Connection connection = connectorConfig.getConnection();
                Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery(PostgresSqlConstant.GETALLSCHEMA)) {
            while (rs.next()) {
                schemaList.add(rs.getString("schema_name"));
            }
        }
        return schemaList;
    }

    private BigInteger initSizeOfConfig(String sizeStr, BigInteger defaultValue) {
        String value = stringToInt(sizeStr);
        int len = sizeStr.length() - value.length();
        if (len > 1) {
            LOGGER.warn("config = {} invalid. Default value:{} byte", sizeStr, defaultValue);
            return defaultValue;
        }
        int size = Integer.parseInt(value);
        return initStoreSize(size, sizeStr, defaultValue);
    }

    private String stringToInt(CharSequence cs) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < cs.length(); i++) {
            if (Character.isDigit(cs.charAt(i))) {
                sb.append(cs.charAt(i));
            }
        }
        return sb.toString();
    }

    private BigInteger initStoreSize(int size, String sizeStr, BigInteger defaultSize) {
        BigInteger unit = BigInteger.valueOf(MEMORY_UNIT);
        if (sizeStr.endsWith("K") || sizeStr.endsWith("k")) {
            return BigInteger.valueOf(size).multiply(unit);
        }
        if (sizeStr.endsWith("M") || sizeStr.endsWith("m")) {
            return BigInteger.valueOf(size).multiply(unit).multiply(unit);
        }
        if (sizeStr.endsWith("G") || sizeStr.endsWith("g")) {
            return BigInteger.valueOf(size).multiply(unit).multiply(unit).multiply(unit);
        }
        LOGGER.warn("config = {} invalid. Default value:{} byte", sizeStr, defaultSize);
        return defaultSize;
    }

    private void pushTruncateMessageForTable(
            RelationalSnapshotContext<PostgresPartition, PostgresOffsetContext> snapshotContext,
            EventDispatcher.SnapshotReceiver receiver, List<String> schemaList) throws InterruptedException {
        for (Iterator<TableId> iterator = snapshotContext.capturedTables.iterator(); iterator.hasNext();) {
            final TableId tableId = iterator.next();
            boolean hasNext = iterator.hasNext();
            if (!schemaList.contains(tableId.schema())) {
                continue;
            }
            ChangeRecordEmitter truncateRecordEmitter = getTruncateRecordEmitter(snapshotContext, tableId);
            dispatcher.dispatchSnapshotEvent(tableId, truncateRecordEmitter, receiver);
        }
    }

    private ChangeRecordEmitter getTruncateRecordEmitter(
            RelationalSnapshotContext<PostgresPartition, PostgresOffsetContext> snapshotContext,
            TableId tableId) {
        ReplicationMessage message = new ReplicationMessage() {
            @Override
            public Operation getOperation() {
                return Operation.TRUNCATE;
            }

            @Override
            public Instant getCommitTime() {
                return clock.currentTime();
            }

            @Override
            public OptionalLong getTransactionId() {
                return OptionalLong.empty();
            }

            @Override
            public String getTable() {
                return null;
            }

            @Override
            public List<Column> getOldTupleList() {
                return new ArrayList<>();
            }

            @Override
            public List<Column> getNewTupleList() {
                return new ArrayList<>();
            }

            @Override
            public boolean hasTypeMetadata() {
                return false;
            }

            @Override
            public boolean isLastEventForLsn() {
                return false;
            }
        };
        snapshotContext.offset.event(tableId, getClock().currentTime());
        return new TruncateRecordEmitter(snapshotContext.partition, snapshotContext.offset, getClock(),
                connectorConfig, schema, jdbcConnection, tableId, message);
    }

    @Override
    public Optional<String> determineSnapshotSelect(
            RelationalSnapshotContext<PostgresPartition, PostgresOffsetContext> snapshotContext,
            TableId tableId) {
        String schemaName = tableId.schema();
        String tableName = tableId.table();
        String fromStatement = " FROM ";
        List<String> childs = getChildTables(schemaName, tableName, connectorConfig.getConnection());
        if (childs.size() > 0) {
            String childTable = childs.get(0);
            Boolean isPartition = isPartitionTable(schemaName, childTable, connectorConfig.getConnection());
            if (!isPartition) {
                fromStatement += " ONLY ";
            }
        }
        List<String> columns = getPreparedColumnNames(schema.tableFor(tableId));
        String query = columns.stream().collect(Collectors.joining(", ", "SELECT ", fromStatement
            + tableId.toDoubleQuotedString()));
        return Optional.of(query);
    }

    private void createDataEventsForTable(PostgresDataEventsParam dataEventsParam, int tableOrder, int tableCount) {
        TableId tableId = dataEventsParam.getTableId();
        RelationalSnapshotContext<PostgresPartition, PostgresOffsetContext> snapshotContext
                = dataEventsParam.getSnapshotContext();
        LOGGER.info("Exporting data from table '{}' ({} of {} tables)", tableId, tableOrder, tableCount);
        String sizeSql = String.format(Locale.ROOT, PostgresSqlConstant.METADATASQL, tableId.schema(),
                tableId.table());
        final Optional<String> selectStatement = determineSnapshotSelect(snapshotContext, tableId);
        if (!selectStatement.isPresent()) {
            LOGGER.warn("For table '{}' the select statement was not provided, skipping table", tableId);
            super.getSnapshotProgressListener().dataCollectionSnapshotCompleted(tableId, 0);
            return;
        }
        try (Connection connection = connectorConfig.getConnection();
             Statement statement = readTableStatementPostgres(connection);
             ResultSet resultSet = statement.executeQuery(sizeSql)) {
            if (resultSet.next()) {
                long size = resultSet.getLong("avgRowLength");
                long totalRows = resultSet.getLong("tableRows");
                int pageRows = size == 0 ? EMPTY_TABLE_PAGEROWS : (int) (pageSize.intValue() / size);
                pageRows = Math.max(pageRows, 1);
                int totalSlice = -1;
                double spacePerSlice = -1;
                if (connectorConfig.isCommitProcess()) {
                    totalSlice = (int) (totalRows / pageRows) + 5;
                    spacePerSlice = (double) pageRows * (double) size;
                }
                lockTable(tableId, statement);
                readAndSendXlogLocation(statement, dataEventsParam);
                LOGGER.info("start export data for table {}.{}", tableId.schema(), tableId.table());
                long limit = pageRows * exportPageNumber;
                long offset = 0L;
                long rows = 0L;
                PostgresPageSliceParam pageSliceParam = new PostgresPageSliceParam();
                pageSliceParam.setPageRows(pageRows);
                pageSliceParam.setTotalSlice(totalSlice);
                pageSliceParam.setSpacePerSlice(spacePerSlice);
                do {
                    String executeSql = selectStatement.get() + String.format(LIMIT_STATEMENT, limit, offset);
                    ResultSet rs = statement.executeQuery(executeSql);
                    rows = processData(dataEventsParam, rs, pageSliceParam);
                    offset += limit;
                    pageSliceParam.addRealTotalRows(rows);
                    pageSliceParam.addSubscript(exportPageNumber);
                } while (limit == rows);
                unLockTable(connection);
                LOGGER.info("\t Finished exporting {} records for table '{}';", pageSliceParam.getRealTotalRows(),
                        tableId);
            }
            connection.rollback();
            createIndexEventsForTable(dataEventsParam, connection);
        } catch (SQLException e) {
            LOGGER.error("Snapshotting of table " + tableId + " failed", e);
        } catch (IOException e) {
            LOGGER.error("IOException", e);
        } catch (InterruptedException e) {
            LOGGER.error("InterruptedException", e);
        }
    }

    private void lockTable(TableId tableId, Statement statement) throws SQLException {
        String schemaName = tableId.schema();
        String tableName = tableId.table();
        String lockStatement = String.format("BEGIN; LOCK TABLE %s.%s IN SHARE MODE;", schemaName, tableName);
        statement.execute(lockStatement);
    }

    private void createIndexEventsForTable(PostgresDataEventsParam dataEventsParam, Connection connection)
            throws IOException, InterruptedException, SQLException {
        TableId tableId = dataEventsParam.getTableId();
        String schemaName = tableId.schema();
        String tableName = tableId.table();

        switchSchema(schemaName, connection);
        Map<String, List<String>> pkInfo = getPkColumns(tableName, connection);
        Map<String, String> indexDef = getIndexDef(tableName, connection);
        List<String> idxDdls = new ArrayList<>();
        if (!pkInfo.isEmpty()) {
            String constraintName = pkInfo.keySet().iterator().next();
            StringJoiner joiner = new StringJoiner(",");
            pkInfo.get(constraintName).forEach((col) -> {
                joiner.add(col);
            });
            String pkCreateStmt = String.format(PostgresSqlConstant.CREATEPK_HEADER,
                    schemaName + "." + tableName, constraintName) + joiner + PostgresSqlConstant.TAIL;
            indexDef.remove(constraintName);
            idxDdls.add(pkCreateStmt);
        }
        idxDdls.addAll(indexDef.values());

        EventDispatcher.SnapshotReceiver receiver = dataEventsParam.getSnapshotReceiver();
        if (indexTaskQueue.size() > workers) {
            dispatchIndexTask(receiver);
        }
        indexTaskQueue.add(new PostgresTableIndexTask(dataEventsParam, idxDdls));
    }

    private Map<String, String> getIndexDef(String table, Connection connection) throws SQLException {
        // key: index name, value: index definition
        Map<String, String> indexInfo = new HashMap<>();
        try (Statement stmt = connection.createStatement();
                ResultSet rst = stmt.executeQuery(String.format(PostgresSqlConstant.GETINDEXINFO, table))) {
            while (rst.next()) {
                indexInfo.put(rst.getString(1), rst.getString(2));
            }
        }
        return indexInfo;
    }

    private Map<String, List<String>> getPkColumns(String table, Connection connection) throws SQLException {
        // key: constraintName, value: pk column list
        Map<String, List<String>> pkInfo = new HashMap<>();
        Set<String> pkSet = new HashSet<>();
        String constraintName = "";
        try (Statement stmt = connection.createStatement();
                ResultSet rst = stmt.executeQuery(String.format(PostgresSqlConstant.GETPKINFO, table))) {
            while (rst.next()) {
                if ("".equals(constraintName)) {
                    constraintName = rst.getString(1);
                }
                pkSet.add(rst.getString(2));
            }
            if (!pkSet.isEmpty()) {
                pkInfo.put(constraintName, new ArrayList<>(pkSet));
            }
        }
        return pkInfo;
    }

    private Statement readTableStatementPostgres(Connection connection) throws SQLException {
        connection.setAutoCommit(false);
        return jdbcConnection.readTableStatementPostgres(connectorConfig, connection);
    }

    private void unLockTable(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("ROLLBACK;");
        }
    }

    private int processData(PostgresDataEventsParam dataEventsParam, ResultSet rs,
                            PostgresPageSliceParam pageSliceParam)
            throws InterruptedException, IOException, SQLException {
        TableId tableId = dataEventsParam.getTableId();
        int rows = 0;
        if (rs.next()) {
            rows = traverseResultSet(dataEventsParam, rs, pageSliceParam);
        } else {
            LOGGER.info("\t Finished exporting {} records for table '{}';", pageSliceParam.getRealTotalRows(),
                    tableId);
        }
        super.getSnapshotProgressListener().dataCollectionSnapshotCompleted(tableId, rows);
        return rows;
    }

    private int traverseResultSet(PostgresDataEventsParam dataEventsParam, ResultSet rs,
                                  PostgresPageSliceParam pageSliceParam)
            throws SQLException, InterruptedException, IOException {
        boolean lastRecordInRst = false;
        TableId tableId = dataEventsParam.getTableId();
        final OptionalLong rowCountForTable = rowCountForTable(tableId);
        ChangeEventSourceContext sourceContext = dataEventsParam.getSourceContext();
        int rows = 0;
        long realTotalRows = pageSliceParam.getRealTotalRows();
        int subscript = pageSliceParam.getSubscript();
        int pageRows = pageSliceParam.getPageRows();
        int totalSlice = pageSliceParam.getTotalSlice();
        double spacePerSlice = pageSliceParam.getSpacePerSlice();
        RelationalSnapshotContext<PostgresPartition, PostgresOffsetContext> snapshotContext
                = dataEventsParam.getSnapshotContext();
        List<String> columnStringArr = new ArrayList<>();
        ColumnUtils.ColumnArray columnArray = ColumnUtils.toArray(rs, snapshotContext.tables.forTable(tableId));
        Threads.Timer logTimer = getTableScanLogTimer();
        while (!lastRecordInRst) {
            if (!sourceContext.isRunning()) {
                throw new InterruptedException("Interrupted while snapshotting table " + tableId);
            }
            rows++;
            realTotalRows++;
            columnStringArr.add(columnToString(rs, columnArray, snapshotContext.tables.forTable(tableId)));
            if (realTotalRows > subscript * pageRows) {
                writeAndSendData(dataEventsParam, columnStringArr, subscript, totalSlice, spacePerSlice);
                if (subscript % MigrationUtil.LOG_REPORT_INTERVAL_SLICES == 0) {
                    LOGGER.info("generated {} slices for table {}, total {} rows", subscript, tableId.table(),
                            realTotalRows);
                }
                subscript++;
                columnStringArr = new ArrayList<>();
            }
            lastRecordInRst = !rs.next();
            if (logTimer.expired()) {
                if (rowCountForTable.isPresent()) {
                    LOGGER.info("\t Exported {} of {} records for table '{}'", realTotalRows,
                            rowCountForTable.getAsLong(), tableId);
                } else {
                    LOGGER.info("\t Exported {} records for table '{}'", realTotalRows, tableId);
                }
                super.getSnapshotProgressListener().rowsScanned(tableId, realTotalRows);
                logTimer = getTableScanLogTimer();
            }
        }
        writeAndSendData(dataEventsParam, columnStringArr, subscript, totalSlice, spacePerSlice);
        LOGGER.info("generate {} slices for table {}, total {} rows", subscript, tableId.table(), realTotalRows);

        synchronized (messLock) {
            ChangeRecordEmitter changeRecordEmitter = getFilePathRecordEmitter(snapshotContext,
                    tableId, new Object[]{"", subscript, totalSlice, spacePerSlice});
            dispatcher.dispatchSnapshotEvent(tableId, changeRecordEmitter,
                    dataEventsParam.getSnapshotReceiver());
        }
        return rows;
    }

    private String columnToString(ResultSet rs, ColumnUtils.ColumnArray columnArray, Table table) throws SQLException {
        StringBuilder stringBuilder = new StringBuilder();
        if (!(dispatcher.getSchema().schemaFor(table.id()) instanceof TableSchema)) {
            throw new DebeziumException("postgresql2opengauss full data schema error");
        }
        final Object[] rowArr = jdbcConnection.rowToArray(table, schema(), rs, columnArray);
        int len = columnArray.getColumns().length;
        for (int i = 0; i < len; i++) {
            Object value = rowArr[i];
            if (value instanceof ByteBuffer) {
                ByteBuffer object = (ByteBuffer) value;
                value = new String(object.array(), object.position(), object.limit(), Charset.defaultCharset());
            }
            if (value instanceof byte[]) {
                StringBuilder bytes = new StringBuilder();
                byte[] obj = (byte[]) value;
                if (obj.length > 0) {
                    bytes.append("\\x");
                }
                bytes.append(HexConverter.convertToHexString(obj));
                value = bytes.toString();
            }
            if (value != null) {
                stringBuilder.append("\"")
                        .append(value.toString().replace("\"", "\"\""))
                        .append("\"").append(DELIMITER);
            } else {
                stringBuilder.append(value).append(DELIMITER);
            }
            if (i == len - 1) {
                stringBuilder.deleteCharAt(stringBuilder.length() - 1);
            }
        }
        return stringBuilder.toString();
    }

    private Object getValue(Column column, Struct newValue) {
        String columnName = column.name();
        int oid = column.jdbcType();
        Object value;
        switch (oid) {
            case Types.BLOB:
                byte[] bytes = newValue.getBytes(columnName);
                value = new String(bytes);
                break;
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                value = newValue.getBytes(columnName);
                break;
            default:
                value = newValue.get(columnName);
        }
        return value;
    }

    private void writeAndSendData(PostgresDataEventsParam dataEventsParam, List<String> columnStringArr,
                                  Integer subscript, int totalSlice, double spacePerSlice)
            throws IOException, InterruptedException {
        TableId tableId = dataEventsParam.getTableId();
        RelationalSnapshotContext<PostgresPartition, PostgresOffsetContext> snapshotContext
                = dataEventsParam.getSnapshotContext();
        EventDispatcher.SnapshotReceiver snapshotReceiver = dataEventsParam.getSnapshotReceiver();
        String path = generateFileName(connectorConfig.databaseName(), tableId.schema(), tableId.table(), subscript);
        if (wirteCsv(columnStringArr, path)) {
            synchronized (messLock) {
                ChangeRecordEmitter changeRecordEmitter = getFilePathRecordEmitter(snapshotContext,
                        tableId, new Object[]{path, subscript, totalSlice, spacePerSlice});
                dispatcher.dispatchSnapshotEvent(tableId, changeRecordEmitter, snapshotReceiver);
            }
        }
    }

    private String generateFileName(String database, String schema, String table, int subscript) {
        return new File(csvPath) + File.separator + String.format(Locale.ROOT, "%s_%s_%s_%d.csv",
                database, schema, table, subscript);
    }

    private boolean wirteCsv(List<String> columnStringArr, String path) throws IOException {
        if (columnStringArr.isEmpty()) {
            return false;
        }
        blockWriteFile();
        File file = new File(path);
        try (FileOutputStream fileInputStream = new FileOutputStream(file);
             PrintWriter printWriter = new PrintWriter(fileInputStream, true)) {
            String data = String.join(System.lineSeparator(), columnStringArr) + System.lineSeparator();
            printWriter.write(data);
            printWriter.flush();
        } catch (IOException e) {
            throw new IOException(e);
        }
        return true;
    }

    private ChangeRecordEmitter getFilePathRecordEmitter(
            RelationalSnapshotContext<PostgresPartition, PostgresOffsetContext> snapshotContext, TableId tableId,
            Object[] row) {
        snapshotContext.offset.event(tableId, getClock().currentTime());
        return new SnapshotChangeFilePathRecordEmitter(snapshotContext.partition, snapshotContext.offset, getClock(),
                    row);
    }

    private ChangeRecordEmitter getIndexRecordEmitter(
            RelationalSnapshotContext<PostgresPartition, PostgresOffsetContext> snapshotContext,
            TableId tableId, String idxDdl) {
        snapshotContext.offset.event(tableId, getClock().currentTime());
        return new SnapshotCreateIndexRecordEmitter(snapshotContext.partition, snapshotContext.offset, getClock(),
                idxDdl);
    }

    private void blockWriteFile() {
        if (csvDirSize == null) {
            return;
        }
        boolean isPrintLog = false;
        for (; ; ) {
            long csvDir = getCsvDir();
            if (csvDir < csvDirSize.longValue()) {
                break;
            } else {
                if (!isPrintLog) {
                    LOGGER.warn("csvDir capacity check, wait to write when satisfied");
                    isPrintLog = true;
                }
            }
            TimeUtil.sleep(1000);
        }
    }

    private long getCsvDir() {
        synchronized (dirLock) {
            return FileUtils.sizeOfDirectory(new File(csvPath));
        }
    }

    @Override
    public void switchSchema(String schema, Connection connection) {
        MigrationUtil.switchSchema(schema, connection);
    }

    protected void dispatchNotifyMessage(RelationalSnapshotContext<PostgresPartition, PostgresOffsetContext> ctx) {
        try {
            dispatcher.dispatchNotifyEvent((receiver -> {
                try {
                    receiver.notifyEvent(getNotifyEvent(ctx));
                } catch (InterruptedException e) {
                    throw new DebeziumException(e);
                }
            }));
        } catch (InterruptedException e) {
            LOGGER.error("dispatch notify message failed.", e);
        }
    }

    private String getParentTables(Table table, Connection connection) {
        String schemaName = table.id().schema();
        String tableName = table.id().table();
        StringJoiner parents = new StringJoiner(",");
        try (Statement stmt = connection.createStatement();
             ResultSet rst = stmt.executeQuery(
                 String.format(PostgresSqlConstant.GET_PARENT_TABLE, schemaName, tableName))) {
            while (rst.next()) {
                parents.add(rst.getString(1));
            }
        } catch (SQLException e) {
            LOGGER.error("get parent tables for table {}.{} failed", schemaName, tableName, e);
        }
        return parents.toString();
    }

    private String getPartitionDdl(Table table, Connection connection) {
        if (ServerVersion.from(serverVersion).getVersionNum()
                < ServerVersion.from(PostgresSqlConstant.PG_SERVER_V10).getVersionNum()) {
            return "";
        }
        String schemaName = table.id().schema();
        String tableName = table.id().table();
        String partitionKey = getPartitionKey(schemaName, tableName, connection);
        if ("".equals(partitionKey)) {
            return "";
        }

        List<String> childs = getChildTables(schemaName, tableName, connection);
        List<PartitionInfo> firstPartitons = getPartitionInfos(childs, partitionKey, schemaName, tableName, connection);
        String partitionDdl = "partition by " + partitionKey + " ";
        if (partitionKey.startsWith(PartitionInfo.RANGE_PARTITION)) {
            partitionDdl += getRangePartitionDdl(firstPartitons);
        } else if (partitionKey.startsWith(PartitionInfo.LIST_PARTITION)) {
            partitionDdl += getListPartitionDdl(firstPartitons);
        } else if (partitionKey.startsWith(PartitionInfo.HASH_PARTITION)) {
            partitionDdl += getHashPartitionDdl(firstPartitons);
        } else {
            throw new IllegalStateException("Unknown partition type: " + partitionKey);
        }
        return partitionDdl;
    }

    private String getRangePartitionDdl(List<PartitionInfo> partitions) {
        Set<String> boundSet = new HashSet<>();
        partitions.forEach(rangePartitionInfo -> {
            boundSet.add(rangePartitionInfo.getRangeLowerBound());
            boundSet.add(rangePartitionInfo.getRangeUpperBound());
        });
        List<String> orderedBoundList = new ArrayList<>(boundSet);
        Collections.sort(orderedBoundList);
        Integer partitionIdx = 1;
        StringBuilder builder = new StringBuilder("( ");
        for (String bound : orderedBoundList) {
            builder.append(String.format("partition p%s values less than %s," + LINESEP, partitionIdx, bound));
            partitionIdx += 1;
        }
        builder.append(String.format("partition p%s values less than (MAXVALUE)" + LINESEP, partitionIdx));
        builder.append(")");
        return builder.toString();
    }

    private String getListPartitionDdl(List<PartitionInfo> partitions) {
        if (partitions.isEmpty()) {
            return "";
        }
        Integer partitionIdx = 1;
        String partitionStr = "( ";
        for (PartitionInfo listPartitionInfo : partitions) {
            partitionStr += String.format("partition p%s values %s," + LINESEP,
                    partitionIdx, listPartitionInfo.getListPartitionValue());
        }
        partitionStr = partitionStr.substring(0, partitionStr.lastIndexOf(","));
        partitionStr += ")";
        return partitionStr;
    }

    private String getHashPartitionDdl(List<PartitionInfo> partitions) {
        if (partitions.isEmpty()) {
            return "";
        }
        Integer partitionIdx = 1;
        String partitionStr = "( ";
        for (int i = 0; i < partitions.size(); ++i) {
            partitionStr += String.format("partition p%s," + LINESEP, partitionIdx);
        }
        partitionStr = partitionStr.substring(0, partitionStr.lastIndexOf(","));
        partitionStr += ")";
        return partitionStr;
    }

    private Set<String> getSubPartitionKeySet(List<PartitionInfo> firstPartitons) {
        Set<String> subPartitionKeySet = new HashSet<>();
        for (PartitionInfo childPartition : firstPartitons) {
            String partitionKey = childPartition.getPartitionKey();
            if (partitionKey == null) {
                continue;
            }
            subPartitionKeySet.add(partitionKey);
        }
        // size=0 means no subPartition
        return subPartitionKeySet;
    }

    private List<PartitionInfo> getPartitionInfos(List<String> partitionTables, String partitionKey, String schema,
                                                  String parentTable, Connection connection) {
        List<PartitionInfo> partitionInfoList = new ArrayList<>();

        for (String table : partitionTables) {
            PartitionInfo partitionInfo;
            if (partitionKey.startsWith(PartitionInfo.RANGE_PARTITION)) {
                partitionInfo = new RangePartitionInfo();
                getRangePartitionValues(schema, table, connection, partitionInfo);
            } else if (partitionKey.startsWith(PartitionInfo.LIST_PARTITION)) {
                partitionInfo = new ListPartitionInfo();
                getListPartitionValues(schema, table, connection, partitionInfo);
            } else if (partitionKey.startsWith(PartitionInfo.HASH_PARTITION)) {
                partitionInfo = new HashPartitionInfo();
                getHashPartitionValues(schema, table, connection, partitionInfo);
            } else {
                throw new IllegalStateException("Unknown partition type: " + partitionKey);
            }
            partitionInfo.setPartitionTable(table);
            partitionInfo.setParentTable(parentTable);
            partitionInfoList.add(partitionInfo);
        }
        return partitionInfoList;
    }

    private void getRangePartitionValues(String schema, String table, Connection connection,
                                         PartitionInfo partitionInfo) {
        if (!(partitionInfo instanceof RangePartitionInfo)) {
            return;
        }
        String rangeExpr = null;
        String subPartitionKey = null;
        try (Statement statement = connection.createStatement();
             ResultSet rst = statement.executeQuery(
                 String.format(PostgresSqlConstant.GET_PARTITION_EXPR, schema, table))) {
            if (rst.next()) {
                subPartitionKey = rst.getString(1);
                rangeExpr = rst.getString(2);
            }
            partitionInfo.setPartitionKey(subPartitionKey);

            if (rangeExpr != null) {
                // 16 is length of "FOR VALUES FROM "
                rangeExpr = rangeExpr.trim().substring(16);
                // idex of "TO"
                int toIdx = rangeExpr.toUpperCase(Locale.ROOT).indexOf("TO");
                String lowerBound = rangeExpr.substring(0, toIdx).trim();
                String upperBound = rangeExpr.substring(toIdx + 2).trim();
                partitionInfo.setRangeLowerBound(lowerBound);
                partitionInfo.setRangeUpperBound(upperBound);
            }
        } catch (SQLException e) {
            LOGGER.error("get range partition value occurred exception", e);
        }
    }

    private void getListPartitionValues(String schema, String table, Connection connection,
                                        PartitionInfo partitionInfo) {
        if (!(partitionInfo instanceof ListPartitionInfo)) {
            return;
        }
        String listExpr = null;
        String subPartitionKey = null;
        try (Statement statement = connection.createStatement();
             ResultSet rst = statement.executeQuery(
                 String.format(PostgresSqlConstant.GET_PARTITION_EXPR, schema, table))) {
            if (rst.next()) {
                subPartitionKey = rst.getString(1);
                listExpr = rst.getString(2);
            }
            partitionInfo.setPartitionKey(subPartitionKey);

            if (listExpr != null) {
                // 14 is length of "FOR VALUES IN "
                listExpr = listExpr.trim().substring(14);
                partitionInfo.setListPartitionValue(listExpr);
            }
        } catch (SQLException e) {
            LOGGER.error("get list partition value occurred exception", e);
        }
    }

    private void getHashPartitionValues(String schema, String table, Connection connection,
                                        PartitionInfo partitionInfo) {
        if (!(partitionInfo instanceof HashPartitionInfo)) {
            return;
        }
        String hashExpr = null;
        String subPartitionKey = null;
        try (Statement statement = connection.createStatement();
             ResultSet rst = statement.executeQuery(
                 String.format(PostgresSqlConstant.GET_PARTITION_EXPR, schema, table))) {
            if (rst.next()) {
                subPartitionKey = rst.getString(1);
                hashExpr = rst.getString(2);
            }
            partitionInfo.setPartitionKey(subPartitionKey);

            if (hashExpr != null) {
                // 16 is length of "FOR VALUES WITH "
                hashExpr = hashExpr.trim().substring(16);
                partitionInfo.setListPartitionValue(hashExpr);
            }
        } catch (SQLException e) {
            LOGGER.error("get hash partition value occurred exception", e);
        }
    }

    private List<String> getChildTables(String schemaName, String tableName, Connection connection) {
        List<String> childs = new ArrayList<>();
        try (Statement stmt = connection.createStatement();
             ResultSet rst = stmt.executeQuery(String.format(
                 PostgresSqlConstant.GET_CHILD_TABLE, schemaName, tableName))) {
            while (rst.next()) {
                childs.add(rst.getString(1));
            }
        } catch (SQLException e) {
            LOGGER.error("get child of table {}.{} failed", schemaName, tableName, e);
        }
        return childs;
    }

    private String getPartitionKey(String schemaName, String tableName, Connection connection) {
        String partitionKey = "";
        try (Statement stmt = connection.createStatement();
             ResultSet rst = stmt.executeQuery(
                 String.format(PostgresSqlConstant.GET_PARTITION_KEY, schemaName, tableName))) {
            if (rst.next()) {
                partitionKey = rst.getString(1);
            }
        } catch (SQLException e) {
            LOGGER.error("get table partition key occurred SQLException.", e);
        }
        return partitionKey == null ? "" : partitionKey.trim().toUpperCase();
    }
}
