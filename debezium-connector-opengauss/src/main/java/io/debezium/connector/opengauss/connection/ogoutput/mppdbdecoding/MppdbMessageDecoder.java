/*
 * Copyright Debezium Authors.
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.opengauss.connection.ogoutput.mppdbdecoding;

import io.debezium.connector.opengauss.OpengaussStreamingChangeEventSource;
import io.debezium.connector.opengauss.OpengaussType;
import io.debezium.connector.opengauss.TypeRegistry;
import io.debezium.connector.opengauss.connection.AbstractMessageDecoder;
import io.debezium.connector.opengauss.connection.MessageDecoderContext;
import io.debezium.connector.opengauss.connection.OpengaussConnection;
import io.debezium.connector.opengauss.connection.ReplicationStream;
import io.debezium.connector.opengauss.connection.Lsn;
import io.debezium.connector.opengauss.connection.TransactionMessage;
import io.debezium.connector.opengauss.connection.ReplicationMessage;
import io.debezium.connector.opengauss.connection.AbstractReplicationMessageColumn;
import io.debezium.connector.opengauss.connection.ogoutput.ColumnMetaData;
import io.debezium.connector.opengauss.connection.ogoutput.OgOutputAndMppdbRelationMetaData;
import io.debezium.connector.opengauss.connection.ogoutput.OgOutputReplicationMessage;
import io.debezium.connector.opengauss.connection.ogoutput.mppdbdecoding.entity.TableStructureEntity;
import org.opengauss.replication.fluent.logical.ChainedLogicalStreamBuilder;
import io.debezium.enums.ErrorCode;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Pattern;

import static java.time.Instant.EPOCH;
import static java.util.stream.Collectors.toMap;

/**
 * MppdbMessageDecoder is responsible for decoding messages in the MPPDB system.
 * It extends AbstractMessageDecoder and provides specific functionality for message decoding.
 *
 * @since 2024-12-11
 */
public class MppdbMessageDecoder extends AbstractMessageDecoder {

    private static final Logger LOGGER = LoggerFactory.getLogger(MppdbMessageDecoder.class);

    private static final Instant PG_EPOCH = LocalDate.of(2000, 1, 1).atStartOfDay().toInstant(ZoneOffset.UTC);

    private static final Pattern TZ_PATTERN = Pattern.compile("([+-]\\d{2})(:?(\\d{2}))?$");

    private final MessageDecoderContext decoderContext;

    private OpengaussConnection connection;

    private Instant commitTimestamp;

    private int currentIndex;

    private int tupleTypeIndex;

    private final TableStructureEntity tableStructure = new TableStructureEntity();

    /**
     * Will be null for a non-transactional decoding message
     */
    private Long transactionId;

    private enum MppdbMessageType {
        BEGIN,
        COMMIT,
        INSERT,
        UPDATE,
        DELETE;

        private static MppdbMessageType forType(char type) {
            switch (type) {
                case 'B':
                    return BEGIN;
                case 'C':
                    return COMMIT;
                case 'I':
                    return INSERT;
                case 'U':
                    return UPDATE;
                case 'D':
                    return DELETE;
                default:
                    throw new IllegalArgumentException("Unsupported message type: " + type);
            }
        }
    }

    private enum TypleType {
        NEW('N'),
        OLD('O'),
        FINAL('F'),
        XID('X'),
        TIME_STAMP('T');

        private final char symbol;

        TypleType(char symbol) {
            this.symbol = symbol;
        }

        public char getSymbol() {
            return symbol;
        }
    }

    public MppdbMessageDecoder(MessageDecoderContext decoderContext) {
        this.decoderContext = decoderContext;
        this.connection = new OpengaussConnection(decoderContext.getConfig(),
            decoderContext.getSchema().getTypeRegistry());
    }

    private void refreshConnection() {
        connection.close();
        connection = new OpengaussConnection(decoderContext.getConfig(), decoderContext.getSchema().getTypeRegistry());
    }

    @Override
    public void processNotEmptyMessage(ByteBuffer buffer, ReplicationStream.ReplicationMessageProcessor processor,
        TypeRegistry typeRegistry) throws SQLException, InterruptedException {
        byte[] source = buffer.array();
        int offset = buffer.arrayOffset();
        int streamByteNumber = buffer.getInt(); // 4
        long lsn = buffer.getLong(); // 8
        final MppdbMessageType messageType = MppdbMessageType.forType((char) buffer.get()); // 1
        switch (messageType) {
            case BEGIN:
                handleBeginMessage(buffer, processor);
                break;
            case COMMIT:
                handleCommitMessage(buffer, processor);
                break;
            case INSERT:
                decodeInsert(buffer, typeRegistry, processor, source, offset);
                break;
            case UPDATE:
                decodeUpdate(buffer, typeRegistry, processor, source, offset);
                break;
            case DELETE:
                decodeDelete(buffer, typeRegistry, processor, source, offset);
                break;
            default:
                LOGGER.trace("Message Type {} skipped, not processed.", messageType);
                break;
        }
    }

    /**
     * Callback handler for the 'B' begin replication message.
     *
     * @implSpec This method handles the 'B' begin replication message and skips it if not processed.
     * @apiNote This method is intended for internal use and may change without notice.
     * @implNote The implementation should decide whether to override this method.
     *
     * @param buffer The replication stream buffer
     * @param processor The replication message processor
     */
    private void handleBeginMessage(ByteBuffer buffer, ReplicationStream.ReplicationMessageProcessor processor)
        throws SQLException, InterruptedException {
        LOGGER.trace("BEGIN CSN: {}", buffer.getLong()); // CSN
        final Lsn lsn = Lsn.valueOf(buffer.getLong()); // LSN
        this.commitTimestamp = EPOCH;
        this.transactionId = 0L;
        byte[] source = buffer.array();
        int offset = buffer.arrayOffset();
        if ((char) buffer.get() == TypleType.TIME_STAMP.getSymbol()) {
            int timeStampLength = buffer.getInt();
            String timeStr = new String(source, offset + buffer.position(), timeStampLength,
                    StandardCharsets.UTF_8);
            Long timeStamp = parseCommitTimestamp(timeStr);
            this.commitTimestamp = PG_EPOCH.plus(timeStamp, ChronoUnit.MICROS);
        }
        LOGGER.trace("Event: {}", MppdbMessageType.BEGIN);
        LOGGER.trace("Final LSN of transaction: {}", lsn);
        LOGGER.trace("Commit timestamp of transaction: {}", commitTimestamp);
        LOGGER.trace("XID of transaction: {}", transactionId);
        processor.process(new TransactionMessage(ReplicationMessage.Operation.BEGIN, transactionId, commitTimestamp));
    }

    private Long parseCommitTimestamp(String timeStr) {
        String normalizedTimeStr = TZ_PATTERN.matcher(timeStr).replaceFirst("$1:00");
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSXXX");
        OffsetDateTime odt = OffsetDateTime.parse(normalizedTimeStr, formatter);
        return odt.toInstant().toEpochMilli();
    }

    /**
     * Callback handler for the 'C' commit replication message.
     *
     * @implSpec This method processes the 'C' commit replication message.
     * @apiNote This method is responsible for handling the commit message in the replication process.
     * @implNote The processor is used to process the transaction message with the specified operation,
     * transaction ID, and commit timestamp.
     *
     * @param buffer The replication stream buffer
     * @param processor The replication message processor
     */
    private void handleCommitMessage(ByteBuffer buffer, ReplicationStream.ReplicationMessageProcessor processor)
        throws SQLException, InterruptedException {
        byte[] source = buffer.array();
        int offset = buffer.arrayOffset();
        char option = (char) buffer.get();
        if (option == TypleType.XID.getSymbol()) {
            this.transactionId = buffer.getLong();
        } else if (option == TypleType.TIME_STAMP.getSymbol()) {
            int timeStampLength = buffer.getInt();
            String timeStr = new String(source, offset + buffer.position(), timeStampLength,
                    StandardCharsets.UTF_8);
            Long timeStamp = parseCommitTimestamp(timeStr);
            this.commitTimestamp = PG_EPOCH.plus(timeStamp, ChronoUnit.MICROS);
        } else {
            return;
        }
        LOGGER.trace("Event: {}", MppdbMessageType.COMMIT);
        LOGGER.trace("Commit timestamp of transaction: {}", commitTimestamp);
        processor.process(new TransactionMessage(ReplicationMessage.Operation.COMMIT, transactionId, commitTimestamp));
    }

    private void decodeInsert(ByteBuffer buffer, TypeRegistry typeRegistry,
                              ReplicationStream.ReplicationMessageProcessor processor,
                              byte[] source, int offset) throws SQLException, InterruptedException {
        setSchemaAndTableName(buffer, source, offset);
        Optional<Table> resolvedTable = resolveRelation(tableStructure.getSchemaName(), tableStructure.getTableName());
        if (!resolvedTable.isPresent()) {
            setTableData(buffer, source, offset);
            initTableMessage(typeRegistry); // init table structure
            processor.process(new ReplicationMessage.NoopMessage(transactionId, commitTimestamp));
            resolvedTable = resolveRelation(tableStructure.getSchemaName(), tableStructure.getTableName());
            currentIndex = tupleTypeIndex;
        }
        if (!resolvedTable.isPresent()) {
            return;
        }
        Table table = resolvedTable.get();
        List<ReplicationMessage.Column> columns = null;
        while (true) {
            setTableData(buffer, source, offset);
            if (tableStructure.getTupleType() == TypleType.FINAL.getSymbol()) {
                break;
            }
            columns = resolveColumnsFromStreamTupleData(typeRegistry, table); // N
        }
        processor.process(new OgOutputReplicationMessage(
                ReplicationMessage.Operation.INSERT,
                table.id().toDoubleQuotedString(),
                commitTimestamp,
                transactionId,
                null,
                columns));
    }

    private void setSchemaAndTableName(ByteBuffer buffer, byte[] source, int offset) {
        currentIndex = buffer.position(); // 14
        int schemaLength = buffer.getShort(currentIndex); // 2
        currentIndex = currentIndex + 2;
        String schemaName = new String(source, offset + currentIndex, schemaLength, StandardCharsets.UTF_8);
        currentIndex = currentIndex + schemaLength;
        int tableNameLength = buffer.getShort(currentIndex); // 2
        currentIndex = currentIndex + 2;
        String tableName = new String(source, offset + currentIndex, tableNameLength, StandardCharsets.UTF_8);
        currentIndex = currentIndex + tableNameLength;
        tupleTypeIndex = currentIndex;
        tableStructure.setSchemaName(schemaName);
        tableStructure.setTableName(tableName);
    }

    private void setTableData(ByteBuffer buffer, byte[] source, int offset) {
        char tupleType = (char) buffer.get(currentIndex); // 1
        currentIndex = currentIndex + 1;
        tableStructure.setTupleType(tupleType); // tupleType
        if (tupleType == TypleType.FINAL.getSymbol()) {
            return;
        }
        short attrNum = buffer.getShort(currentIndex); // 2
        currentIndex = currentIndex + 2;
        List<String> columnNameList = new ArrayList<>();
        List<Integer> oidList = new ArrayList<>();
        List<String> colValueList = new ArrayList<>();
        for (int i = 0; i < attrNum; i++) {
            int columnNameLength = buffer.getShort(currentIndex); // 2
            currentIndex = currentIndex + 2;
            String columnName = new String(source, offset + currentIndex, columnNameLength, StandardCharsets.UTF_8);
            // Unquotes the columnName if it is a keyword, eg: date, time
            columnName = Strings.unquoteIdentifierPart(columnName);
            columnNameList.add(columnName);
            currentIndex = currentIndex + columnNameLength;
            int oid = buffer.getInt(currentIndex); // 4
            oidList.add(oid);
            currentIndex = currentIndex + 4;
            int currentColumnValueLength = buffer.getInt(currentIndex); // 4
            currentIndex = currentIndex + 4;
            if (currentColumnValueLength == 0xFFFFFFFF || currentColumnValueLength == 0) {
                LOGGER.trace("Current column value length is 0.");
                currentColumnValueLength = 0;
                colValueList.add(null);
            } else {
                String colValue = new String(source, offset + currentIndex, currentColumnValueLength,
                    StandardCharsets.UTF_8);
                colValueList.add(colValue);
            }
            currentIndex = currentIndex + currentColumnValueLength;
        }
        tableStructure.setAttrNum(attrNum); // attrNum
        tableStructure.setColumnName(columnNameList); // columnNameList
        tableStructure.setOid(oidList); // oidList
        tableStructure.setColValue(colValueList); // colValueList
    }

    private void decodeUpdate(ByteBuffer buffer, TypeRegistry typeRegistry,
                              ReplicationStream.ReplicationMessageProcessor processor,
                              byte[] source, int offset) throws SQLException, InterruptedException {
        setSchemaAndTableName(buffer, source, offset);
        Optional<Table> resolvedTable = resolveRelation(tableStructure.getSchemaName(), tableStructure.getTableName());
        if (!resolvedTable.isPresent()) {
            setTableData(buffer, source, offset);
            initTableMessage(typeRegistry);
            processor.process(new ReplicationMessage.NoopMessage(transactionId, commitTimestamp));
            resolvedTable = resolveRelation(tableStructure.getSchemaName(), tableStructure.getTableName());
            currentIndex = tupleTypeIndex;
        }
        if (!resolvedTable.isPresent()) {
            return;
        }
        Table table = resolvedTable.get();
        List<ReplicationMessage.Column> columns = null;
        List<ReplicationMessage.Column> oldColumns = null;
        while (true) {
            setTableData(buffer, source, offset);
            char tupleType = tableStructure.getTupleType(); // 1, N or O or F
            if (tupleType == TypleType.NEW.getSymbol()) {
                columns = resolveColumnsFromStreamTupleData(typeRegistry, table);
            } else if (tupleType == TypleType.OLD.getSymbol()) {
                oldColumns = resolveColumnsFromStreamTupleData(typeRegistry, table);
            } else {
                break;
            }
        }
        processor.process(new OgOutputReplicationMessage(
                ReplicationMessage.Operation.UPDATE,
                table.id().toDoubleQuotedString(),
                commitTimestamp,
                transactionId,
                oldColumns,
                columns));
    }

    private void decodeDelete(ByteBuffer buffer, TypeRegistry typeRegistry,
                              ReplicationStream.ReplicationMessageProcessor processor,
                              byte[] source, int offset) throws SQLException, InterruptedException {
        setSchemaAndTableName(buffer, source, offset);
        Optional<Table> resolvedTable = resolveRelation(tableStructure.getSchemaName(), tableStructure.getTableName());
        if (!resolvedTable.isPresent()) {
            setTableData(buffer, source, offset);
            initTableMessage(typeRegistry);
            processor.process(new ReplicationMessage.NoopMessage(transactionId, commitTimestamp));
            resolvedTable = resolveRelation(tableStructure.getSchemaName(), tableStructure.getTableName());
            currentIndex = tupleTypeIndex;
        }
        if (!resolvedTable.isPresent()) {
            return;
        }
        Table table = resolvedTable.get();
        List<ReplicationMessage.Column> oldColumns = null;
        while (true) {
            setTableData(buffer, source, offset);
            if (tableStructure.getTupleType() == TypleType.FINAL.getSymbol()) {
                break;
            }
            oldColumns = resolveColumnsFromStreamTupleData(typeRegistry, table); // O
        }
        processor.process(new OgOutputReplicationMessage(
                ReplicationMessage.Operation.DELETE,
                table.id().toDoubleQuotedString(),
                commitTimestamp,
                transactionId,
                oldColumns,
                null));
    }

    @Override
    public ChainedLogicalStreamBuilder optionsWithMetadata(ChainedLogicalStreamBuilder builder,
        Function<Integer, Boolean> hasMinimumServerVersion) {
        builder = builder.withSlotOption("proto_version", 1)
                .withSlotOption("publication_names", decoderContext.getConfig().publicationName());
        // DBZ-4374 Use enum once the driver got updated
        if (hasMinimumServerVersion.apply(140000)) {
            builder = builder.withSlotOption("messages", true);
        }
        return builder;
    }

    @Override
    public ChainedLogicalStreamBuilder optionsWithoutMetadata(ChainedLogicalStreamBuilder builder,
        Function<Integer, Boolean> hasMinimumServerVersion) {
        return builder;
    }

    private Optional<Table> resolveRelation(String schemaName, String tableName) {
        return Optional.ofNullable(decoderContext.getSchema().tableFor(schemaName, tableName));
    }

    private void initTableMessage(TypeRegistry typeRegistry) throws SQLException {
        if (!connection.connection().isValid(1)) {
            refreshConnection();
        }
        String schemaName = tableStructure.getSchemaName();
        Map<String, Optional<String>> columnDefaults;
        Map<String, Boolean> columnOptionality = new HashMap<>(); // 修复后：声明并初始化
        List<String> primaryKeyColumns;
        String tableName = tableStructure.getTableName();
        final DatabaseMetaData databaseMetadata = connection.connection().getMetaData();
        final TableId tableId = new TableId(null, schemaName, tableName);
        final List<Column> readColumns = getTableColumnsFromDatabase(connection, databaseMetadata, tableId);
        columnDefaults = readColumns.stream().filter(Column::hasDefaultValue)
            .collect(toMap(Column::name, Column::defaultValueExpression));
        columnOptionality = readColumns.stream().collect(toMap(Column::name, Column::isOptional));
        primaryKeyColumns = connection.readPrimaryKeyNames(databaseMetadata, tableId);
        if (primaryKeyColumns == null || primaryKeyColumns.isEmpty()) {
            LOGGER.warn("Primary keys are not defined for table '{}', defaulting to unique indices",
                tableStructure.getTableName());
            primaryKeyColumns = connection.readTableUniqueIndices(databaseMetadata, tableId);
        }
        Map<String, Integer> columnDimension = readColumns.stream()
                .collect(toMap(column -> column.name(), column -> column.dimension(),
                        (duplicateKey, newValue) -> newValue));
        List<ColumnMetaData> columns = new ArrayList<>();
        Set<String> columnNames = new HashSet<>();
        for (short i = 0; i < tableStructure.getAttrNum(); ++i) {
            String columnName = tableStructure.getColumnName().get(i);
            Boolean optional = columnOptionality.get(columnName);
            if (optional == null) {
                LOGGER.warn("Column '{}' optionality could not be determined, defaulting to true", columnName);
                optional = true;
            }
            int attypmod = -1;
            boolean key = isColumnInPrimaryKey(schemaName, tableName, columnName, primaryKeyColumns);
            final boolean hasDefault = columnDefaults.containsKey(columnName);
            final String defaultValueExpression = columnDefaults.getOrDefault(columnName,
                Optional.empty()).orElse(null);
            final OpengaussType postgresType = typeRegistry.get(tableStructure.getOid().get(i));
            Integer dimension = columnDimension.getOrDefault(columnName, 0);
            columns.add(new ColumnMetaData(columnName, postgresType, key, optional, hasDefault,
                defaultValueExpression, attypmod, dimension));
            columnNames.add(columnName);
        }
        primaryKeyColumns.retainAll(columnNames);
        Table table = resolveRelationFromMetadata(new OgOutputAndMppdbRelationMetaData(0, schemaName, tableName,
            columns, primaryKeyColumns));
        decoderContext.getSchema().applySchemaChangesForTable(table);
    }

    /**
     * Resolve the replication stream's tuple data to a list of replication message columns.
     *
     * @param typeRegistry The database type registry
     * @param table The database table
     * @return list of replication message columns
     */
    private List<ReplicationMessage.Column> resolveColumnsFromStreamTupleData(TypeRegistry typeRegistry, Table table) {
        List<ReplicationMessage.Column> columns = new ArrayList<>(tableStructure.getAttrNum());
        for (short i = 0; i < tableStructure.getAttrNum(); ++i) {
            final Column column = table.columns().get(i);
            final String columnName = column.name();
            final String typeName = column.typeName();
            final OpengaussType columnType = typeRegistry.get(typeName);
            final String typeExpression = column.typeExpression();
            final boolean optional = column.isOptional();
            String finalValueStr = tableStructure.getColValue().get(i);
            columns.add(
                new AbstractReplicationMessageColumn(columnName, columnType, typeExpression, optional, true) {
                    @Override
                    public Object getValue(OpengaussStreamingChangeEventSource.PgConnectionSupplier connection,
                        boolean includeUnknownDatatypes) {
                        return OgOutputReplicationMessage.getValue(columnName, columnType, typeExpression,
                            finalValueStr, connection, includeUnknownDatatypes, typeRegistry);
                    }
                    @Override
                    public String toString() {
                        return columnName + "(" + typeExpression + ")=" + finalValueStr;
                    }
                });
        }
        columns.forEach(c -> LOGGER.trace("Column: {}", c));
        return columns;
    }

    private boolean isColumnInPrimaryKey(String schemaName, String tableName, String columnName,
        List<String> primaryKeyColumns) {
        if (!primaryKeyColumns.isEmpty() && primaryKeyColumns.contains(columnName)) {
            return true;
        } else if (primaryKeyColumns.isEmpty()) {
            // The table's metadata was either not fetched or table no longer has a primary key
            // Lets attempt to use the known schema primary key configuration as a fallback
            Table existingTable = decoderContext.getSchema().tableFor(new TableId(null, schemaName, tableName));
            return existingTable != null && existingTable.primaryKeyColumnNames().contains(columnName);
        } else {
            return false;
        }
    }

    /**
     * Constructs a {@link Table} based on the supplied {@link OgOutputAndMppdbRelationMetaData}.
     *
     * @param metadata The relation metadata collected from previous 'R' replication stream messages
     * @return table based on a prior replication relation message
     */
    private Table resolveRelationFromMetadata(OgOutputAndMppdbRelationMetaData metadata) {
        List<Column> columns = new ArrayList<>();
        for (ColumnMetaData columnMetadata : metadata.getColumns()) {
            ColumnEditor editor = Column.editor()
                .name(columnMetadata.getColumnName())
                .jdbcType(columnMetadata.getPostgresType().getRootType().getJdbcId())
                .nativeType(columnMetadata.getPostgresType().getRootType().getOid())
                .optional(columnMetadata.isOptional())
                .type(columnMetadata.getPostgresType().getName(), columnMetadata.getTypeName())
                .length(columnMetadata.getLength())
                .scale(columnMetadata.getScale());
            if (columnMetadata.hasDefaultValue()) {
                editor.defaultValueExpression(columnMetadata.getDefaultValueExpression());
            }
            columns.add(editor.create());
        }
        Table table = Table.editor()
                .addColumns(columns)
                .setPrimaryKeyNames(metadata.getPrimaryKeyNames())
                .tableId(metadata.getTableId())
                .create();
        LOGGER.trace("Resolved '{}' as '{}'", table.id(), table);
        return table;
    }

    private List<Column> getTableColumnsFromDatabase(OpengaussConnection connection, DatabaseMetaData databaseMetadata,
        TableId tableId) throws SQLException {
        List<Column> readColumns = new ArrayList<>();
        try {
            connection.readTableColumnDimension(tableId.schema(), tableId.table());
            try (ResultSet columnMetadata = databaseMetadata.getColumns(
                    null, tableId.schema(), tableId.table(), null
            )) {
                while (columnMetadata.next()) {
                    connection.readColumnForDecoder(
                        columnMetadata, tableId, decoderContext.getConfig().getColumnFilter()
                        ).ifPresent(readColumns::add);
                }
            }
        } catch (SQLException e) {
            LOGGER.error("{}Failed to read column metadata for '{}.{}'", ErrorCode.SQL_EXCEPTION, tableId.schema(),
                tableId.table());
            throw e;
        }
        return readColumns;
    }
}
