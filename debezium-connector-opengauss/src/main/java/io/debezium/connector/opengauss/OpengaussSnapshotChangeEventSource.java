/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.opengauss;

import io.debezium.DebeziumException;
import io.debezium.connector.opengauss.connection.Lsn;
import io.debezium.connector.opengauss.connection.OpengaussConnection;
import io.debezium.connector.opengauss.connection.ReplicationMessage;
import io.debezium.connector.opengauss.process.OgFullSourceProcessInfo;
import io.debezium.connector.opengauss.process.OgProcessCommitter;
import io.debezium.connector.opengauss.process.ProgressStatus;
import io.debezium.connector.opengauss.process.TableInfo;
import io.debezium.connector.opengauss.spi.SlotCreationResult;
import io.debezium.connector.opengauss.spi.SlotState;
import io.debezium.connector.opengauss.spi.Snapshotter;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.relational.RelationalSnapshotChangeEventSource;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchema;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.schema.SchemaChangeEvent.SchemaChangeEventType;
import io.debezium.util.Clock;
import io.debezium.util.ColumnUtils;
import io.debezium.util.Threads;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Description: Opengauss database is changed as a remote snapshot
 *
 * @author czy
 * @since 2023-06-07
 */
public class OpengaussSnapshotChangeEventSource extends RelationalSnapshotChangeEventSource<OpengaussPartition, OpengaussOffsetContext> {
    private static final Logger LOGGER = LoggerFactory.getLogger(OpengaussSnapshotChangeEventSource.class);
    private static final String DELIMITER = ",";
    private static final int MEMORY_UNIT = 1024;
    private static final String METADATASQL = "select"
            + "    c.relname tableName,"
            + "    c.reltuples tableRows,"
            + "    case"
            + "        when c.reltuples > 0 then pg_table_size(c.oid) / c.reltuples"
            + "        else 0"
            + "    end as avgRowLength "
            + " from"
            + "    pg_class c"
            + "    LEFT JOIN pg_namespace n on n.oid = c.relnamespace"
            + " where"
            + "    n.nspname = '%s' "
            + "    and c.relname = '%s' "
            + " order by"
            + "    c.reltuples asc;";

    private final OpengaussConnectorConfig connectorConfig;
    private final OpengaussConnection jdbcConnection;
    private final OpengaussSchema schema;
    private final Snapshotter snapshotter;
    private final SlotCreationResult slotCreatedInfo;
    private final SlotState startingSlotInfo;
    private final Object messLock = new Object();
    private final Object dirLock = new Object();
    private String csvPath;
    private int csvDirSize;
    private int pageSize = 2 * 1024 * 1024;
    private AtomicInteger unlockCount = new AtomicInteger(0);

    public OpengaussSnapshotChangeEventSource(OpengaussConnectorConfig connectorConfig, Snapshotter snapshotter,
                                              OpengaussConnection jdbcConnection, OpengaussSchema schema, EventDispatcher<TableId> dispatcher, Clock clock,
                                              SnapshotProgressListener snapshotProgressListener, SlotCreationResult slotCreatedInfo, SlotState startingSlotInfo) {
        super(connectorConfig, jdbcConnection, schema, dispatcher, clock, snapshotProgressListener);
        this.connectorConfig = connectorConfig;
        this.jdbcConnection = jdbcConnection;
        this.schema = schema;
        this.snapshotter = snapshotter;
        this.slotCreatedInfo = slotCreatedInfo;
        this.startingSlotInfo = startingSlotInfo;
        this.csvPath = connectorConfig.getExportCsvPath();
    }

    @Override
    protected SnapshottingTask getSnapshottingTask(OpengaussOffsetContext previousOffset) {
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
    protected SnapshotContext<OpengaussPartition, OpengaussOffsetContext> prepare(OpengaussPartition partition)
            throws Exception {
        return new PostgresSnapshotContext(partition, connectorConfig.databaseName());
    }

    @Override
    protected void connectionCreated(RelationalSnapshotContext<OpengaussPartition, OpengaussOffsetContext> snapshotContext)
            throws Exception {
        // If using catch up streaming, the connector opens the transaction that the snapshot will eventually use
        // before the catch up streaming starts. By looking at the current wal location, the transaction can determine
        // where the catch up streaming should stop. The transaction is held open throughout the catch up
        // streaming phase so that the snapshot is performed from a consistent view of the data. Since the isolation
        // level on the transaction used in catch up streaming has already set the isolation level and executed
        // statements, the transaction does not need to get set the level again here.
        if (snapshotter.shouldStreamEventsStartingFromSnapshot() && startingSlotInfo == null) {
            setSnapshotTransactionIsolationLevel();
        }
        schema.refresh(jdbcConnection, false);
    }

    @Override
    protected Set<TableId> getAllTableIds(RelationalSnapshotContext<OpengaussPartition, OpengaussOffsetContext> ctx)
            throws Exception {
        return jdbcConnection.readTableNames(ctx.catalogName, null, null, new String[]{ "TABLE" });
    }

    @Override
    protected void lockTablesForSchemaSnapshot(ChangeEventSourceContext sourceContext,
                                               RelationalSnapshotContext<OpengaussPartition, OpengaussOffsetContext> snapshotContext)
            throws SQLException, InterruptedException {
        final Duration lockTimeout = connectorConfig.snapshotLockTimeout();
        final Optional<String> lockStatement = snapshotter.snapshotTableLockingStatement(lockTimeout, snapshotContext.capturedTables);

        if (lockStatement.isPresent()) {
            LOGGER.info("Waiting a maximum of '{}' seconds for each table lock", lockTimeout.getSeconds());
            jdbcConnection.executeWithoutCommitting(lockStatement.get());
            // now that we have the locks, refresh the schema
            schema.refresh(jdbcConnection, false);
        }
    }

    @Override
    protected void releaseSchemaSnapshotLocks(RelationalSnapshotContext<OpengaussPartition, OpengaussOffsetContext> snapshotContext)
            throws SQLException {
    }

    @Override
    protected void releaseDataSnapshotLocks(RelationalSnapshotContext<OpengaussPartition,
        OpengaussOffsetContext> snapshotContext) throws Exception {
        jdbcConnection.executeWithoutCommitting("COMMIT;");
    }

    @Override
    protected void determineSnapshotOffset(RelationalSnapshotContext<OpengaussPartition, OpengaussOffsetContext> ctx,
        OpengaussOffsetContext previousOffset) throws Exception {
        OpengaussOffsetContext offset = ctx.offset;
        if (offset == null) {
            if (previousOffset != null && !snapshotter.shouldStreamEventsStartingFromSnapshot()) {
                // The connect framework, not the connector, manages triggering committing offset state so the
                // replication stream may not have flushed the latest offset state during catch up streaming.
                // The previousOffset variable is shared between the catch up streaming and snapshot phases and
                // has the latest known offset state.
                offset = OpengaussOffsetContext.initialContext(connectorConfig, jdbcConnection, getClock(),
                        previousOffset.lastCommitLsn(), previousOffset.lastCompletelyProcessedLsn());
            }
            else {
                offset = OpengaussOffsetContext.initialContext(connectorConfig, jdbcConnection, getClock());
            }
            ctx.offset = offset;
        }

        updateOffsetForSnapshot(offset);
    }

    private void updateOffsetForSnapshot(OpengaussOffsetContext offset) throws SQLException {
        final Lsn xlogStart = getTransactionStartLsn();
        final long txId = jdbcConnection.currentTransactionId().longValue();
        LOGGER.info("Read xlogStart at '{}' from transaction '{}'", xlogStart, txId);

        // use the old xmin, as we don't want to update it if in xmin recovery
        offset.updateWalPosition(xlogStart, offset.lastCompletelyProcessedLsn(), clock.currentTime(), txId, offset.xmin(), null);
    }

    protected void updateOffsetForPreSnapshotCatchUpStreaming(OpengaussOffsetContext offset) throws SQLException {
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
                                      RelationalSnapshotContext<OpengaussPartition, OpengaussOffsetContext> snapshotContext,
                                      OpengaussOffsetContext offsetContext) throws SQLException, InterruptedException {
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
                    connectorConfig.getTableFilters().dataCollectionFilter(),
                    null,
                    false);
        }
        schema.refresh(jdbcConnection, false);
    }

    @Override
    protected SchemaChangeEvent getCreateTableEvent(RelationalSnapshotContext<OpengaussPartition, OpengaussOffsetContext> snapshotContext,
                                                    Table table)
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

    /**
     * Duplicating full data acquisition to achieve multithreaded data acquisition
     *
     * @param sourceContext ChangeEventSourceContext
     * @param snapshotContext RelationalSnapshotContext
     * @throws Exception InterruptedException
     */
    @Override
    protected void createDataEvents(ChangeEventSourceContext sourceContext,
        RelationalSnapshotContext<OpengaussPartition, OpengaussOffsetContext> snapshotContext) throws Exception {
        if (!new File(csvPath).exists()) {
            Files.createDirectories(Paths.get(csvPath));
        }
        if (connectorConfig.getExportCsvPathSize() != null) {
            csvDirSize = initCsvDirSize();
        }
        if (connectorConfig.getExportFileSize() != null) {
            pageSize = initPagePartitionSize();
        }
        EventDispatcher.SnapshotReceiver receiver = dispatcher.getSnapshotChangeEventReceiver();
        List<String> schemaList = appointSchemas();
        pushTruncateMessageForTable(snapshotContext, receiver, schemaList);
        tryStartingSnapshot(snapshotContext);
        Set<TableId> tableIds = snapshotContext.capturedTables.stream().filter(o -> schemaList.contains(o.schema()))
                .collect(Collectors.toSet());
        final int tableCount = tableIds.size();
        AtomicInteger tableOrder = new AtomicInteger(1);
        LOGGER.info("Snapshotting contents of {} tables while still in transaction", tableCount);
        // init poolExecutor
        ThreadPoolExecutor poolExecutor = new ThreadPoolExecutor(tableCount, tableCount,
                5, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
        OgFullSourceProcessInfo ogFullSourceProcessInfo = new OgFullSourceProcessInfo();
        ogFullSourceProcessInfo.setTotal(tableCount);
        for (Iterator<TableId> tableIdIterator = tableIds.iterator();
            tableIdIterator.hasNext();) {
            final TableId tableId = tableIdIterator.next();
            boolean isLastTable = !tableIdIterator.hasNext();

            if (!sourceContext.isRunning()) {
                throw new InterruptedException("Interrupted while snapshotting table " + tableId);
            }

            LOGGER.debug("Snapshotting table {}", tableId);
            poolExecutor.execute(() -> {
                OpenGaussDataEventsParam dataEventsParam = new OpenGaussDataEventsParam(sourceContext, snapshotContext,
                        receiver, snapshotContext.tables.forTable(tableId), isLastTable);
                createOpenGaussDataEventsForTable(dataEventsParam, tableOrder.getAndIncrement(), tableCount,
                        ogFullSourceProcessInfo);
            });
        }
        // Wait for data collection to complete
        while (poolExecutor.getTaskCount() != poolExecutor.getCompletedTaskCount()) {
            Thread.sleep(1000);
        }
        poolExecutor.shutdown();
        snapshotContext.offset.preSnapshotCompletion();
        receiver.completeSnapshot();
        snapshotContext.offset.postSnapshotCompletion();
    }

    @Override
    protected void complete(SnapshotContext<OpengaussPartition, OpengaussOffsetContext> snapshotContext) {
        snapshotter.snapshotCompleted();
    }

    /**
     * Generate a valid Postgres query string for the specified table and columns
     *
     * @param tableId the table to generate a query for
     * @return a valid query string
     */
    @Override
    protected Optional<String> getSnapshotSelect(RelationalSnapshotContext<OpengaussPartition, OpengaussOffsetContext> snapshotContext,
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
    private static class PostgresSnapshotContext extends RelationalSnapshotContext<OpengaussPartition, OpengaussOffsetContext> {

        public PostgresSnapshotContext(OpengaussPartition partition, String catalogName) throws SQLException {
            super(partition, catalogName);
        }
    }

    /**
     * Generate truncate table message to push to the queue.
     *
     * @param snapshotContext RelationalSnapshotContext
     * @param receiver SnapshotReceiver
     * @throws InterruptedException Link break
     */
    private void pushTruncateMessageForTable(RelationalSnapshotContext<OpengaussPartition,
            OpengaussOffsetContext> snapshotContext,
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

    private List<String> appointSchemas() throws SQLException {
        List<String> schemaList = new ArrayList<>();
        try (Connection connection = connectorConfig.getConnection(connectorConfig);
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery("SELECT pn.oid AS schema_oid, iss.catalog_name, iss.schema_owner, "
                    + "iss.schema_name FROM information_schema.schemata iss "
                    + "INNER JOIN pg_namespace pn ON pn.nspname = iss.schema_name "
                    + "where iss.schema_name = 'public' or pn.oid > 16384;")) {
            while (rs.next()) {
                schemaList.add(rs.getString("schema_name"));
            }
        }
        return schemaList;
    }

    private int initCsvDirSize() {
        String csvPathSize = connectorConfig.getExportCsvPathSize();
        int size;
        if (isNumeric(csvPathSize)) {
            size = Integer.parseInt(csvPathSize);
            return size * MEMORY_UNIT * MEMORY_UNIT * MEMORY_UNIT;
        } else {
            size = Integer.parseInt(stringToInt(csvPathSize));
        }
        LOGGER.info("config: export.csv.path.size = {}", csvPathSize);
        return initStoreSize(size, csvPathSize, csvDirSize);
    }

    private int initPagePartitionSize() {
        String exportFileSize = connectorConfig.getExportFileSize();
        int size;
        if (isNumeric(exportFileSize)) {
            size = Integer.parseInt(exportFileSize);
            return size * MEMORY_UNIT * MEMORY_UNIT;
        } else {
            size = Integer.parseInt(stringToInt(exportFileSize));
        }
        LOGGER.info("config: export.file.size = {}", exportFileSize);
        return initStoreSize(size, exportFileSize, pageSize);
    }

    private int initStoreSize(int size, String sizeStr, int defaultSize) {
        if (sizeStr.endsWith("K") || sizeStr.endsWith("k")) {
            return size * MEMORY_UNIT;
        }
        if (sizeStr.endsWith("M") || sizeStr.endsWith("m")) {
            return size * MEMORY_UNIT * MEMORY_UNIT;
        }
        if (sizeStr.endsWith("G") || sizeStr.endsWith("g")) {
            return size * MEMORY_UNIT * MEMORY_UNIT * MEMORY_UNIT;
        }
        LOGGER.info("config = {} invalid. Default value:{} byte", sizeStr, defaultSize);
        return defaultSize;
    }

    private boolean isNumeric(CharSequence cs) {
        int size = cs.length();
        if (size == 0) {
            return false;
        }
        for (int i = 0; i < size; i++) {
            if (!Character.isDigit(cs.charAt(i))) {
                return false;
            }
        }
        return true;
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

    private ChangeRecordEmitter getTruncateRecordEmitter(RelationalSnapshotContext<OpengaussPartition,
        OpengaussOffsetContext> snapshotContext, TableId tableId) {
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

    /**
     * The task logic first divides the file size according to the data volume of the table,
     * writes the collected data into the corresponding file, and then generates a message,
     * which contains the file location and table information, and pushes the message to the queue.
     *
     * @param dataEventsParam OpenGaussDataEventsParam
     * @param tableOrder int
     * @param tableCount int
     */
    private void createOpenGaussDataEventsForTable(OpenGaussDataEventsParam dataEventsParam, int tableOrder,
        int tableCount, OgFullSourceProcessInfo ogFullSourceProcessInfo) {
        Table table = dataEventsParam.getTable();
        RelationalSnapshotContext<OpengaussPartition,
                OpengaussOffsetContext> snapshotContext = dataEventsParam.getSnapshotContext();
        LOGGER.info("Exporting data from table '{}' ({} of {} tables)", table.id(), tableOrder, tableCount);
        String sizeSql = String.format(Locale.ROOT, METADATASQL, table.id().schema(), table.id().table());
        final Optional<String> selectStatement = determineSnapshotSelect(snapshotContext, table.id());
        if (!selectStatement.isPresent()) {
            LOGGER.warn("For table '{}' the select statement was not provided, skipping table", table.id());
            super.getSnapshotProgressListener().dataCollectionSnapshotCompleted(table.id(), 0);
            return;
        }
        try (Connection connection = connectorConfig.getConnection(connectorConfig);
            Statement statement = readTableStatementOpengauss(connection);
            ResultSet resultSet = statement.executeQuery(sizeSql)) {
            if (resultSet.next()) {
                long size = resultSet.getLong("avgRowLength");
                int pageRows = size == 0 ? 100000 : (int) (pageSize / size);
                ResultSet rs = statement.executeQuery(selectStatement.get());
                unLockTable(tableCount, dataEventsParam.getSnapshotContext());
                processData(dataEventsParam, rs, pageRows, ogFullSourceProcessInfo, size);
            }
        } catch (SQLException e) {
            LOGGER.error("Snapshotting of table " + table.id() + " failed", e);
        } catch (IOException e) {
            LOGGER.error("IOException", e);
        } catch (InterruptedException e) {
            LOGGER.error("InterruptedException", e);
        } catch (Exception e) {
            LOGGER.error("UNLOCK TABLES error", e);
        }
    }

    private void unLockTable(int tableCount, RelationalSnapshotContext<OpengaussPartition,
        OpengaussOffsetContext> snapshotContext) throws Exception {
        int count = unlockCount.incrementAndGet();
        if (tableCount == count) {
            releaseDataSnapshotLocks(snapshotContext);
            LOGGER.info("UNLOCK TABLES.");
        }
    }

    private void sourceTableReport(OgFullSourceProcessInfo ogFullSourceProcessInfo, long size,
        int tableRows, String tableName, String schema) {
        List<TableInfo> tableList = ogFullSourceProcessInfo.getTableList();
        TableInfo tableInfo = new TableInfo();
        tableInfo.setRecord(tableRows);
        tableInfo.setData((double) (size * tableRows) / MEMORY_UNIT);
        tableInfo.setName(tableName);
        tableInfo.setSchema(schema);
        tableInfo.updateStatus(ProgressStatus.NOT_MIGRATED);
        synchronized (messLock) {
            tableList.add(tableInfo);
        }
        if (tableList.size() == ogFullSourceProcessInfo.getTotal()) {
            OgProcessCommitter ogProcessCommitter = new OgProcessCommitter(connectorConfig, OgProcessCommitter.REVERSE_FULL_PROCESS_SUFFIX);
            ogProcessCommitter.commitSourceTableProcessInfo(ogFullSourceProcessInfo);
        }
    }

    private void processData(OpenGaussDataEventsParam dataEventsParam, ResultSet rs, int pageRows,
                             OgFullSourceProcessInfo ogFullSourceProcessInfo, long size)
        throws InterruptedException, IOException, SQLException {
        Table table = dataEventsParam.getTable();
        RelationalSnapshotContext<OpengaussPartition,
                OpengaussOffsetContext> snapshotContext = dataEventsParam.getSnapshotContext();
        boolean isLastTable = dataEventsParam.isLastTable();
        int rows = 0;
        if (rs.next()) {
            rows = traverseResultSet(dataEventsParam, rs, pageRows, isLastTable);
        } else if (isLastTable) {
            snapshotContext.lastTable = isLastTable;
            snapshotContext.lastRecordInTable = false;
            lastSnapshotRecord(snapshotContext);
        } else {
            LOGGER.info("\t Finished exporting {} records for table '{}';", rows, table.id());
        }
        sourceTableReport(ogFullSourceProcessInfo, size, rows,
                table.id().table(), table.id().schema());
        LOGGER.info("\t Finished exporting {} records for table '{}';", rows, table.id());
        super.getSnapshotProgressListener().dataCollectionSnapshotCompleted(table.id(), rows);
    }

    private int traverseResultSet(OpenGaussDataEventsParam dataEventsParam, ResultSet rs, int pageRows,
        boolean isLastTable) throws SQLException, InterruptedException, IOException {
        Threads.Timer logTimer = getTableScanLogTimer();
        boolean lastRecordInTable = false;
        Table table = dataEventsParam.getTable();
        final OptionalLong rowCountForTable = rowCountForTable(table.id());
        RelationalSnapshotContext<OpengaussPartition,
                OpengaussOffsetContext> snapshotContext = dataEventsParam.getSnapshotContext();
        ChangeEventSourceContext sourceContext = dataEventsParam.getSourceContext();
        int rows = 0;
        int subscript = 1;
        List<String> columnNames = super.getPreparedColumnNames(table);
        String columnString = columnNames.stream().map(o -> o.replaceAll("\"", ""))
                .collect(Collectors.joining(DELIMITER));
        List<String> columnStringArr = new ArrayList<>();
        ColumnUtils.ColumnArray columnArray = ColumnUtils.toArray(rs, table);
        while (!lastRecordInTable) {
            if (!sourceContext.isRunning()) {
                throw new InterruptedException("Interrupted while snapshotting table " + table.id());
            }
            rows++;
            columnStringArr.add(columnToString(rs, columnArray, table));
            if (rows > subscript * pageRows) {
                wirteAndSendData(dataEventsParam, columnStringArr, subscript, columnString);
                subscript++;
                columnStringArr = new ArrayList<>();
            }
            // final Object[] row = jdbcConnection.rowToArray(table, schema(), rs, columnArray);
            lastRecordInTable = !rs.next();
            if (logTimer.expired()) {
                if (rowCountForTable.isPresent()) {
                    LOGGER.info("\t Exported {} of {} records for table '{}'", rows, rowCountForTable.getAsLong(),
                            table.id());
                } else {
                    LOGGER.info("\t Exported {} records for table '{}'", rows, table.id());
                }
                super.getSnapshotProgressListener().rowsScanned(table.id(), rows);
                logTimer = getTableScanLogTimer();
            }
            if (isLastTable && lastRecordInTable) {
                snapshotContext.lastTable = isLastTable;
                snapshotContext.lastRecordInTable = lastRecordInTable;
                lastSnapshotRecord(snapshotContext);
            }
        }
        if (rows <= subscript * pageRows) {
            wirteAndSendData(dataEventsParam, columnStringArr, subscript, columnString);
        }
        return rows;
    }


    private void wirteAndSendData(OpenGaussDataEventsParam dataEventsParam, List<String> columnStringArr, int subscript,
        String columnString) throws IOException, InterruptedException {
        Table table = dataEventsParam.getTable();
        RelationalSnapshotContext<OpengaussPartition,
            OpengaussOffsetContext> snapshotContext = dataEventsParam.getSnapshotContext();
        EventDispatcher.SnapshotReceiver snapshotReceiver = dataEventsParam.getSnapshotReceiver();
        String path = generateFileName(table.id().schema(), table.id().table(), subscript);
        if (wirteCsv(columnStringArr, path)) {
            synchronized (messLock) {
                ChangeRecordEmitter changeRecordEmitter = getFilePathRecordEmitter(snapshotContext,
                        table.id(), new String[]{path, columnString});
                dispatcher.dispatchSnapshotEvent(table.id(), changeRecordEmitter, snapshotReceiver);
            }
        }
    }

    private String generateFileName(String schema, String table, int subscript) {
        return new File(csvPath) + File.separator + String.format(Locale.ROOT, "%s_%s_%d.csv",
                schema, table, subscript);
    }

    private ChangeRecordEmitter getFilePathRecordEmitter(RelationalSnapshotContext<OpengaussPartition,
            OpengaussOffsetContext> snapshotContext, TableId tableId, String[] row) {
        snapshotContext.offset.event(tableId, getClock().currentTime());
        return new SnapshotChangeFilePathRecordEmitter(snapshotContext.partition, snapshotContext.offset, getClock(),
                row);
    }

    private String columnToString(ResultSet rs, ColumnUtils.ColumnArray columnArray, Table table) throws SQLException {
        StringBuilder stringBuilder = new StringBuilder();
        TableSchema tableSchema;
        if (dispatcher.getSchema().schemaFor(table.id()) instanceof TableSchema) {
            tableSchema = (TableSchema) dispatcher.getSchema().schemaFor(table.id());
        } else {
            throw new DebeziumException("opengauss2mysql full data schema error");
        }
        Struct newValue = tableSchema.valueFromColumnData(jdbcConnection.rowToArray(table, schema(), rs, columnArray));
        int len = columnArray.getColumns().length;
        for (int i = 0; i < len; i++) {
            Object value = newValue.get(columnArray.getColumns()[i].name());
            if (value instanceof ByteBuffer) {
                ByteBuffer object = (ByteBuffer) value;
                value = new String(object.array(), object.position(), object.limit(), Charset.defaultCharset());
            }
            if (value instanceof byte[]) {
                StringBuilder bytes = new StringBuilder();
                byte[] obj = (byte[]) value;
                for (byte b : obj) {
                    bytes.append(String.valueOf(b));
                }
                value = bytes.toString();
            }
            stringBuilder.append(value);
            if (i != len - 1) {
                stringBuilder.append(DELIMITER);
            }
        }
        return stringBuilder.toString();
    }

    private boolean wirteCsv(List<String> columnStringArr, String path) throws IOException {
        if (columnStringArr.isEmpty()) {
            return false;
        }
        blockWriteFile();
        File file = new File(path);
        try (FileOutputStream fileInputStream = new FileOutputStream(file);) {
            PrintWriter printWriter = new PrintWriter(fileInputStream, true);
            String data = String.join(System.lineSeparator(), columnStringArr) + System.lineSeparator();
            printWriter.write(data);
            printWriter.flush();
        } catch (IOException e) {
            throw new IOException(e);
        }
        return true;
    }

    private void blockWriteFile() {
        if (csvDirSize == 0) {
            return;
        }
        for (;;) {
            long csvDir = getCsvDir();
            if (csvDir < csvDirSize) {
                break;
            }
        }
    }

    private long getCsvDir() {
        synchronized (dirLock) {
            return FileUtils.sizeOfDirectory(new File(csvPath));
        }
    }

    private Statement readTableStatementOpengauss(Connection connection) throws SQLException {
        return jdbcConnection.readTableStatementOpengauss(connectorConfig, connection);
    }

}
