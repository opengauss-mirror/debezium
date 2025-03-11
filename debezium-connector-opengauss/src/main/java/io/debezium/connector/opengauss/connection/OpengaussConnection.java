/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.opengauss.connection;

import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.kafka.connect.errors.ConnectException;
import org.opengauss.core.BaseConnection;
import org.opengauss.jdbc.PgConnection;
import org.opengauss.jdbc.TimestampUtils;
import org.opengauss.replication.LogSequenceNumber;
import org.opengauss.util.PGmoney;
import org.opengauss.util.PSQLState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.annotation.VisibleForTesting;
import io.debezium.config.Configuration;
import io.debezium.connector.opengauss.OgOid;
import io.debezium.connector.opengauss.OpengaussConnectorConfig;
import io.debezium.connector.opengauss.OpengaussSchema;
import io.debezium.connector.opengauss.OpengaussType;
import io.debezium.connector.opengauss.OpengaussValueConverter;
import io.debezium.connector.opengauss.TypeRegistry;
import io.debezium.connector.opengauss.spi.SlotState;
import io.debezium.data.SpecialValueDecimal;
import io.debezium.enums.ErrorCode;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.schema.DatabaseSchema;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;

/**
 * {@link JdbcConnection} connection extension used for connecting to Postgres instances.
 *
 * @author Horia Chiorean
 */
public class OpengaussConnection extends JdbcConnection {
    protected static final ConnectionFactory FACTORY = getFactory();
    private static final String URL_PATTERN = "jdbc:opengauss://${" + JdbcConfiguration.HOSTNAME + "}:${"
            + JdbcConfiguration.PORT + "}/${" + JdbcConfiguration.DATABASE + "}";

    /**
     * Obtaining a replication slot may fail if there's a pending transaction. We're retrying to get a slot for 30 min.
     */
    private static final int MAX_ATTEMPTS_FOR_OBTAINING_REPLICATION_SLOT = 900;

    private static final Duration PAUSE_BETWEEN_REPLICATION_SLOT_RETRIEVAL_ATTEMPTS = Duration.ofSeconds(2);

    private static Logger LOGGER = LoggerFactory.getLogger(OpengaussConnection.class);

    private final TypeRegistry typeRegistry;
    private final OpengaussDefaultValueConverter defaultValueConverter;

    private Map<TableId, Map<String, Integer>> tableColumnDimension = new HashMap<>();

    private static ConnectionFactory getFactory() {
        return  (config) -> {
            String url_pattern = URL_PATTERN;

            Properties properties = config.asProperties();
            String isCluster = properties.getProperty(JdbcConfiguration.ISCLUSTER.toString());
            String standbyHostnames = properties.getProperty(JdbcConfiguration.STANDBY_HOSTNAMES.toString());
            String standbyPorts = properties.getProperty(JdbcConfiguration.STANDBY_PORTS.toString());

            if (OpengaussConnectorConfig.isOpengaussClusterAvailable(isCluster, standbyHostnames, standbyPorts)) {
                url_pattern = "jdbc:opengauss://${" + JdbcConfiguration.HOSTNAME + "}:${" + JdbcConfiguration.PORT + "}"
                        + OpengaussConnectorConfig.getUrlFragment(standbyHostnames, standbyPorts)
                        + "/${" + JdbcConfiguration.DATABASE + "}?targetServerType=master";
            }

            return JdbcConnection.patternBasedFactory(url_pattern, org.opengauss.Driver.class.getName(),
                            OpengaussConnection.class.getClassLoader(),
                            JdbcConfiguration.PORT.withDefault(OpengaussConnectorConfig.PORT.defaultValueAsString()))
                    .connect(config);
        };
    }

    /**
     * Creates a Postgres connection using the supplied configuration.
     * If necessary this connection is able to resolve data type mappings.
     * Such a connection requires a {@link OpengaussValueConverter}, and will provide its own {@link TypeRegistry}.
     * Usually only one such connection per connector is needed.
     *
     * @param config {@link Configuration} instance, may not be null.
     * @param valueConverterBuilder supplies a configured {@link OpengaussValueConverter} for a given {@link TypeRegistry}
     */
    public OpengaussConnection(Configuration config, PostgresValueConverterBuilder valueConverterBuilder) {
        super(config, FACTORY, OpengaussConnection::validateServerVersion, OpengaussConnection::defaultSettings, "\"", "\"");

        if (Objects.isNull(valueConverterBuilder)) {
            this.typeRegistry = null;
            this.defaultValueConverter = null;
        }
        else {
            this.typeRegistry = new TypeRegistry(this);

            final OpengaussValueConverter valueConverter = valueConverterBuilder.build(this.typeRegistry);
            this.defaultValueConverter = new OpengaussDefaultValueConverter(valueConverter, this.getTimestampUtils());
        }
    }

    /**
     * Create a Postgres connection using the supplied configuration and {@link TypeRegistry}
     * @param config {@link Configuration} instance, may not be null.
     * @param typeRegistry an existing/already-primed {@link TypeRegistry} instance
     */
    public OpengaussConnection(OpengaussConnectorConfig config, TypeRegistry typeRegistry) {
        super(config.getJdbcConfig(), FACTORY, OpengaussConnection::validateServerVersion, OpengaussConnection::defaultSettings, "\"", "\"");
        if (Objects.isNull(typeRegistry)) {
            this.typeRegistry = null;
            this.defaultValueConverter = null;
        }
        else {
            this.typeRegistry = typeRegistry;
            final OpengaussValueConverter valueConverter = OpengaussValueConverter.of(config, this.getDatabaseCharset(), typeRegistry);
            this.defaultValueConverter = new OpengaussDefaultValueConverter(valueConverter, this.getTimestampUtils());
        }
    }

    /**
     * Creates a Postgres connection using the supplied configuration.
     * The connector is the regular one without datatype resolution capabilities.
     *
     * @param config {@link Configuration} instance, may not be null.
     */
    public OpengaussConnection(Configuration config) {
        this(config, null);
    }

    /**
     * Returns a JDBC connection string for the current configuration.
     *
     * @return a {@code String} where the variables in {@code urlPattern} are replaced with values from the configuration
     */
    public String connectionString() {
        return connectionString(URL_PATTERN);
    }

    /**
     * Prints out information about the REPLICA IDENTITY status of a table.
     * This in turn determines how much information is available for UPDATE and DELETE operations for logical replication.
     *
     * @param tableId the identifier of the table
     * @return the replica identity information; never null
     * @throws SQLException if there is a problem obtaining the replica identity information for the given table
     */
    public ServerInfo.ReplicaIdentity readReplicaIdentityInfo(TableId tableId) throws SQLException {
        String statement = "SELECT relreplident FROM pg_catalog.pg_class c " +
                "LEFT JOIN pg_catalog.pg_namespace n ON c.relnamespace=n.oid " +
                "WHERE n.nspname=? and c.relname=?";
        String schema = tableId.schema() != null && tableId.schema().length() > 0 ? tableId.schema() : "public";
        StringBuilder replIdentity = new StringBuilder();
        prepareQuery(statement, stmt -> {
            stmt.setString(1, schema);
            stmt.setString(2, tableId.table());
        }, rs -> {
            if (rs.next()) {
                replIdentity.append(rs.getString(1));
            }
            else {
                LOGGER.warn("Cannot determine REPLICA IDENTITY information for table '{}'", tableId);
            }
        });
        return ServerInfo.ReplicaIdentity.parseFromDB(replIdentity.toString());
    }

    @Override
    public Set<String> querySystemSchema() throws SQLException {
        // query system schemas based pg_namespace oid < 16384
        // and two extra schema dolphin_catalog and performance_schema.
        HashSet<String> systemSchemaSet = new HashSet<>();
        String sql = "select nspname from pg_namespace where oid < 16384 and nspname != 'public';";
        query(sql, rs -> {
            while(rs.next()) {
                systemSchemaSet.add(rs.getString("nspname"));
            }
        });
        systemSchemaSet.add("dolphin_catalog");
        systemSchemaSet.add("performance_schema");
        return systemSchemaSet;
    }

    @Override
    public Set<TableId> readTableNames(String databaseCatalog, String schemaNamePattern, String tableNamePattern,
        String[] tableTypes)
        throws SQLException {
        return filterTables();
    }

    @Override
    protected void readTableColumnMetadata(Tables tables, DatabaseMetaData metadata, Map<TableId,
            List<Column>> columnsByTable) throws SQLException {
        List<String> schemaList = columnsByTable.keySet().stream().map(TableId::schema).
                distinct().collect(Collectors.toList());
        HashMap<String, List<String>> tablePkMap = getTablePkMap(schemaList);
        // Read the metadata for the primary keys ...
        for (Map.Entry<TableId, List<Column>> tableEntry : columnsByTable.entrySet()) {
            // First get the primary key information, which must be done for *each* table ...
            String fullName = String.format("{%s}.{%s}", tableEntry.getKey().schema(), tableEntry.getKey().table());
            List<String> pkColumnNames = tablePkMap.getOrDefault(fullName, Collections.emptyList());

            // Then define the table ...
            List<Column> columns = tableEntry.getValue();
            Collections.sort(columns);
            String defaultCharsetName = null; // JDBC does not expose character sets
            tables.overwriteTable(tableEntry.getKey(),
                    columns,
                    pkColumnNames,
                    defaultCharsetName);
        }
        LOGGER.warn("finish query get table column other meta data");
    }

    /**
     * Reads the column dimension of a table
     *
     * @param schemaNamePattern schemaNamePattern
     * @param tableName tableName
     * @throws SQLException exception
     */
    public void readTableColumnDimension(String schemaNamePattern, String tableName) throws SQLException {
        String sql = "SELECT "
                + "n.nspname, c.relname, a.attname, a.attndims "
                + "FROM "
                + "pg_catalog.pg_attribute a "
                + "JOIN pg_catalog.pg_class c ON a.attrelid = c.oid "
                + "JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid "
                + "JOIN pg_catalog.pg_type t ON a.atttypid = t.oid "
                + "WHERE ";
        if (schemaNamePattern != null) {
            sql = sql + "n.nspname like '" + schemaNamePattern + "' AND ";
        }
        if (tableName != null) {
            sql = sql + "c.relname like '" + tableName + "' AND ";
        }
        sql = sql + "a.attnum > 0 "
                + "AND NOT a.attisdropped "
                + "AND n.nspname NOT IN ('pg_catalog', 'information_schema');";
        prepareQuery(sql, stmt -> {}, rs -> {
            while (rs.next()) {
                String schema = rs.getString("nspname");
                String table = rs.getString("relname");
                String columnName = rs.getString("attname");
                int dimension = rs.getInt("attndims");
                TableId tableId = new TableId(null, schema, table);
                tableColumnDimension.computeIfAbsent(tableId, id -> new HashMap<>()).put(columnName, dimension);
            }
        });
    }

    private HashMap<String, List<String>> getTablePkMap(List<String> schemaList) throws SQLException {
        LOGGER.warn("schema is {}", schemaList);
        String sql = "select c.relname tableName,ns.nspname schemaName,ns.oid,a.attname columnName from pg_class c " +
                "left join pg_namespace ns on c.relnamespace=ns.oid left join pg_attribute a on c.oid=a.attrelid " +
                "and a.attnum>0 and not a.attisdropped inner join pg_constraint cs on a.attrelid=cs.conrelid " +
                "and a.attnum=any(cs.conkey) where ns.nspname= ? and (cs.contype='p' or cs.contype='u');";
        LOGGER.warn(sql);
        HashMap<String, List<String>> tablePkMap = new HashMap<>();
        for (String schema: schemaList) {
            prepareQuery(sql, stmt -> stmt.setString(1, schema), rs -> {
                while (rs.next()) {
                    String schemaName = rs.getString("schemaName");
                    String tableName = rs.getString("tableName");
                    String tableFullName = String.format("{%s}.{%s}", schemaName, tableName);
                    String columnName = rs.getString("columnName");
                    if (tablePkMap.containsKey(tableFullName)) {
                        tablePkMap.get(tableFullName).add(columnName);
                    }
                    else {
                        ArrayList<String> list = new ArrayList<>();
                        list.add(columnName);
                        tablePkMap.put(tableFullName, list);
                    }
                }
            });
        }
        return tablePkMap;
    }

    /**
     * Returns the current state of the replication slot
     * @param slotName the name of the slot
     * @param pluginName the name of the plugin used for the desired slot
     * @return the {@link SlotState} or null, if no slot state is found
     * @throws SQLException
     */
    public SlotState getReplicationSlotState(String slotName, String pluginName) throws SQLException {
        ServerInfo.ReplicationSlot slot;
        try {
            slot = readReplicationSlotInfo(slotName, pluginName);
            if (slot.equals(ServerInfo.ReplicationSlot.INVALID)) {
                return null;
            }
            else {
                return slot.asSlotState();
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ConnectException("Interrupted while waiting for valid replication slot info", e);
        }
    }

    /**
     * Fetches the state of a replication stage given a slot name and plugin name
     * @param slotName the name of the slot
     * @param pluginName the name of the plugin used for the desired slot
     * @return the {@link ServerInfo.ReplicationSlot} object or a {@link ServerInfo.ReplicationSlot#INVALID} if
     *         the slot is not valid
     * @throws SQLException is thrown by the underlying JDBC
     */
    private ServerInfo.ReplicationSlot fetchReplicationSlotInfo(String slotName, String pluginName) throws SQLException {
        final String database = database();
        final ServerInfo.ReplicationSlot slot = queryForSlot(slotName, database, pluginName,
                rs -> {
                    if (rs.next()) {
                        boolean active = rs.getBoolean("active");
                        final Lsn confirmedFlushedLsn = parseConfirmedFlushLsn(slotName, pluginName, database, rs);
                        if (confirmedFlushedLsn == null) {
                            return null;
                        }
                        Lsn restartLsn = parseRestartLsn(slotName, pluginName, database, rs);
                        if (restartLsn == null) {
                            return null;
                        }
                        final Long xmin = rs.getLong("catalog_xmin");
                        return new ServerInfo.ReplicationSlot(active, confirmedFlushedLsn, restartLsn, xmin);
                    }
                    else {
                        LOGGER.debug("No replication slot '{}' is present for plugin '{}' and database '{}'", slotName,
                                pluginName, database);
                        return ServerInfo.ReplicationSlot.INVALID;
                    }
                });
        return slot;
    }

    /**
     * Fetches a replication slot, repeating the query until either the slot is created or until
     * the max number of attempts has been reached
     *
     * To fetch the slot without the retries, use the {@link OpengaussConnection#fetchReplicationSlotInfo} call
     * @param slotName the slot name
     * @param pluginName the name of the plugin
     * @return the {@link ServerInfo.ReplicationSlot} object or a {@link ServerInfo.ReplicationSlot#INVALID} if
     *         the slot is not valid
     * @throws SQLException is thrown by the underyling jdbc driver
     * @throws InterruptedException is thrown if we don't return an answer within the set number of retries
     */
    @VisibleForTesting
    ServerInfo.ReplicationSlot readReplicationSlotInfo(String slotName, String pluginName) throws SQLException, InterruptedException {
        final String database = database();
        final Metronome metronome = Metronome.parker(PAUSE_BETWEEN_REPLICATION_SLOT_RETRIEVAL_ATTEMPTS, Clock.SYSTEM);

        for (int attempt = 1; attempt <= MAX_ATTEMPTS_FOR_OBTAINING_REPLICATION_SLOT; attempt++) {
            final ServerInfo.ReplicationSlot slot = fetchReplicationSlotInfo(slotName, pluginName);
            if (slot != null) {
                LOGGER.info("Obtained valid replication slot {}", slot);
                return slot;
            }
            LOGGER.warn(
                    "Cannot obtain valid replication slot '{}' for plugin '{}' and database '{}' [during attempt {} out of {}, concurrent tx probably blocks taking snapshot.",
                    slotName, pluginName, database, attempt, MAX_ATTEMPTS_FOR_OBTAINING_REPLICATION_SLOT);
            metronome.pause();
        }

        throw new ConnectException("Unable to obtain valid replication slot. "
                + "Make sure there are no long-running transactions running in parallel as they may hinder the allocation of the replication slot when starting this connector");
    }

    protected ServerInfo.ReplicationSlot queryForSlot(String slotName, String database, String pluginName,
                                                      ResultSetMapper<ServerInfo.ReplicationSlot> map)
            throws SQLException {
        return prepareQueryAndMap("select * from pg_replication_slots where slot_name = ? and database = ? and plugin = ?", statement -> {
            statement.setString(1, slotName);
            statement.setString(2, database);
            statement.setString(3, pluginName);
        }, map);
    }

    /**
     * Obtains the LSN to resume streaming from. On PG 9.5 there is no confirmed_flushed_lsn yet, so restart_lsn will be
     * read instead. This may result in more records to be re-read after a restart.
     */
    private Lsn parseConfirmedFlushLsn(String slotName, String pluginName, String database, ResultSet rs) {
        Lsn confirmedFlushedLsn = null;

        try {
            confirmedFlushedLsn = tryParseLsn(slotName, pluginName, database, rs, "confirmed_flush_lsn");
        }
        catch (SQLException e) {
            LOGGER.info("unable to find confirmed_flushed_lsn, falling back to restart_lsn");
            try {
                confirmedFlushedLsn = tryParseLsn(slotName, pluginName, database, rs, "restart_lsn");
            }
            catch (SQLException e2) {
                throw new ConnectException("Neither confirmed_flush_lsn nor restart_lsn could be found");
            }
        }

        return confirmedFlushedLsn;
    }

    private Lsn parseRestartLsn(String slotName, String pluginName, String database, ResultSet rs) {
        Lsn restartLsn = null;
        try {
            restartLsn = tryParseLsn(slotName, pluginName, database, rs, "restart_lsn");
        }
        catch (SQLException e) {
            throw new ConnectException("restart_lsn could be found");
        }

        return restartLsn;
    }

    private Lsn tryParseLsn(String slotName, String pluginName, String database, ResultSet rs, String column) throws ConnectException, SQLException {
        Lsn lsn = null;

        String lsnStr = rs.getString(column);
        if (lsnStr == null) {
            return null;
        }
        try {
            lsn = Lsn.valueOf(lsnStr);
        }
        catch (Exception e) {
            throw new ConnectException("Value " + column + " in the pg_replication_slots table for slot = '"
                    + slotName + "', plugin = '"
                    + pluginName + "', database = '"
                    + database + "' is not valid. This is an abnormal situation and the database status should be checked.");
        }
        if (!lsn.isValid()) {
            throw new ConnectException("Invalid LSN returned from database");
        }
        return lsn;
    }

    /**
     * Drops a replication slot that was created on the DB
     *
     * @param slotName the name of the replication slot, may not be null
     * @return {@code true} if the slot was dropped, {@code false} otherwise
     */
    public boolean dropReplicationSlot(String slotName) {
        final int ATTEMPTS = 3;
        for (int i = 0; i < ATTEMPTS; i++) {
            try {
                execute("select pg_drop_replication_slot('" + slotName + "')");
                return true;
            }
            catch (SQLException e) {
                // slot is active
                if (PSQLState.OBJECT_IN_USE.getState().equals(e.getSQLState())) {
                    if (i < ATTEMPTS - 1) {
                        LOGGER.debug("Cannot drop replication slot '{}' because it's still in use", slotName);
                    }
                    else {
                        LOGGER.warn("Cannot drop replication slot '{}' because it's still in use", slotName);
                        return false;
                    }
                }
                else if (PSQLState.UNDEFINED_OBJECT.getState().equals(e.getSQLState())) {
                    LOGGER.debug("Replication slot {} has already been dropped", slotName);
                    return false;
                }
                else {
                    LOGGER.error("{}Unexpected error while attempting to drop replication slot",
                        ErrorCode.SQL_EXCEPTION, e);
                    return false;
                }
            }
            try {
                Metronome.parker(Duration.ofSeconds(1), Clock.system()).pause();
            }
            catch (InterruptedException e) {
            }
        }
        return false;
    }

    /**
     * Drops the debezium publication that was created.
     *
     * @param publicationName the publication name, may not be null
     * @return {@code true} if the publication was dropped, {@code false} otherwise
     */
    public boolean dropPublication(String publicationName) {
        try {
            LOGGER.debug("Dropping publication '{}'", publicationName);
            execute("DROP PUBLICATION " + publicationName);
            return true;
        }
        catch (SQLException e) {
            if (PSQLState.UNDEFINED_OBJECT.getState().equals(e.getSQLState())) {
                LOGGER.debug("Publication {} has already been dropped", publicationName);
            }
            else {
                LOGGER.error("{}Unexpected error while attempting to drop publication", ErrorCode.SQL_EXCEPTION, e);
            }
            return false;
        }
    }

    @Override
    public synchronized void close() {
        try {
            super.close();
        }
        catch (SQLException e) {
            LOGGER.error("{}Unexpected error while closing Postgres connection", ErrorCode.DB_CONNECTION_EXCEPTION, e);
        }
    }

    /**
     * Returns the PG id of the current active transaction
     *
     * @return a PG transaction identifier, or null if no tx is active
     * @throws SQLException if anything fails.
     */
    public Long currentTransactionId() throws SQLException {
        AtomicLong txId = new AtomicLong(0);
        query("select * from txid_current()", rs -> {
            if (rs.next()) {
                txId.compareAndSet(0, rs.getLong(1));
            }
        });
        long value = txId.get();
        return value > 0 ? value : null;
    }

    /**
     * Returns the current position in the server tx log.
     *
     * @return a long value, never negative
     * @throws SQLException if anything unexpected fails.
     */
    public long currentXLogLocation() throws SQLException {
        AtomicLong result = new AtomicLong(0);
        int majorVersion = connection().getMetaData().getDatabaseMajorVersion();
        query(majorVersion >= 10 ? "select * from pg_current_wal_lsn()" : "select * from pg_current_xlog_location()", rs -> {
            if (!rs.next()) {
                throw new IllegalStateException("there should always be a valid xlog position");
            }
            result.compareAndSet(0, LogSequenceNumber.valueOf(rs.getString(1)).asLong());
        });
        return result.get();
    }

    /**
     * Returns information about the PG server to which this instance is connected.
     *
     * @return a {@link ServerInfo} instance, never {@code null}
     * @throws SQLException if anything fails
     */
    public ServerInfo serverInfo() throws SQLException {
        ServerInfo serverInfo = new ServerInfo();
        query("SELECT version(), current_user, current_database()", rs -> {
            if (rs.next()) {
                serverInfo.withServer(rs.getString(1)).withUsername(rs.getString(2)).withDatabase(rs.getString(3));
            }
        });
        return serverInfo;
    }

    public Charset getDatabaseCharset() {
        try {
            return Charset.forName(((BaseConnection) connection()).getEncoding().name());
        }
        catch (SQLException e) {
            throw new DebeziumException("Couldn't obtain encoding for database " + database(), e);
        }
    }

    public TimestampUtils getTimestampUtils() {
        try {
            return ((PgConnection) this.connection()).getTimestampUtils();
        }
        catch (SQLException e) {
            throw new DebeziumException("Couldn't get timestamp utils from underlying connection", e);
        }
    }

    protected static void defaultSettings(Configuration.Builder builder) {
        // we require Postgres 9.4 as the minimum server version since that's where logical replication was first introduced
        builder.with("assumeMinServerVersion", "9.4");
    }

    private static void validateServerVersion(Statement statement) {
    }

    @Override
    public String quotedColumnIdString(String columnName) {
        if (columnName.contains("\"")) {
            columnName = columnName.replaceAll("\"", "\"\"");
        }

        return super.quotedColumnIdString(columnName);
    }

    @Override
    protected int resolveNativeType(String typeName) {
        return getTypeRegistry().get(typeName).getRootType().getOid();
    }

    @Override
    protected int resolveJdbcType(int metadataJdbcType, int nativeType) {
        // Special care needs to be taken for columns that use user-defined domain type data types
        // where resolution of the column's JDBC type needs to be that of the root type instead of
        // the actual column to properly influence schema building and value conversion.
        return getTypeRegistry().get(nativeType).getRootType().getJdbcId();
    }

    @Override
    protected Map<TableId, List<Column>> getColumnsDetails(String databaseCatalog, String schemaNamePattern,
                                                           String tableName, Tables.TableFilter tableFilter,
                                                           Tables.ColumnNameFilter columnFilter,
                                                           DatabaseMetaData metadata, final Set<TableId> viewIds)
            throws SQLException {
        Map<TableId, List<Column>> columnsByTable = new HashMap<>();
        readTableColumnDimension(schemaNamePattern, tableName);
        try (ResultSet columnMetadata = metadata.getColumns(databaseCatalog, schemaNamePattern, tableName, null)) {
            while (columnMetadata.next()) {
                String catalogName = resolveCatalogName(columnMetadata.getString(1));
                String schemaName = columnMetadata.getString(2);
                String metaTableName = columnMetadata.getString(3);
                TableId tableId = new TableId(catalogName, schemaName, metaTableName);

                // exclude views and non-captured tables
                if (viewIds.contains(tableId) || (tableFilter != null && !tableFilter.isIncluded(tableId))) {
                    continue;
                }

                // add all included columns
                readTableColumn(columnMetadata, tableId, columnFilter).ifPresent(column -> {
                    columnsByTable.computeIfAbsent(tableId, t -> new ArrayList<>())
                            .add(column.create());
                });
            }
        }
        return columnsByTable;
    }


    @Override
    protected Optional<ColumnEditor> readTableColumn(ResultSet columnMetadata, TableId tableId, Tables.ColumnNameFilter columnFilter) throws SQLException {
        return doReadTableColumn(columnMetadata, tableId, columnFilter);
    }

    public Optional<Column> readColumnForDecoder(ResultSet columnMetadata, TableId tableId, Tables.ColumnNameFilter columnNameFilter)
            throws SQLException {
        return doReadTableColumn(columnMetadata, tableId, columnNameFilter).map(ColumnEditor::create);
    }

    private Optional<ColumnEditor> doReadTableColumn(ResultSet columnMetadata, TableId tableId, Tables.ColumnNameFilter columnFilter)
            throws SQLException {
        Map<String, Integer> columnDimensions = tableColumnDimension.get(tableId);
        final String columnName = columnMetadata.getString(4);
        Integer dimension = columnDimensions.get(columnName);
        if (columnFilter == null || columnFilter.matches(tableId.catalog(), tableId.schema(), tableId.table(), columnName)) {
            final ColumnEditor column = Column.editor().name(columnName);
            column.type(columnMetadata.getString(6));

            // first source the length/scale from the column metadata provided by the driver
            // this may be overridden below if the column type is a user-defined domain type
            column.length(columnMetadata.getInt(7));
            if (columnMetadata.getObject(9) != null) {
                column.scale(columnMetadata.getInt(9));
            }

            column.optional(isNullable(columnMetadata.getInt(11)));
            column.position(columnMetadata.getInt(17));
            column.autoIncremented("YES".equalsIgnoreCase(columnMetadata.getString(23)));
            column.dimension(dimension);
            String autogenerated = null;
            try {
                autogenerated = columnMetadata.getString(24);
            }
            catch (SQLException e) {
                // ignore, some drivers don't have this index - e.g. Postgres
            }
            column.generated("YES".equalsIgnoreCase(autogenerated));

            // Lookup the column type from the TypeRegistry
            // For all types, we need to set the Native and Jdbc types by using the root-type
            final OpengaussType nativeType = getTypeRegistry().get(column.typeName());
            column.nativeType(nativeType.getRootType().getOid());
            column.jdbcType(nativeType.getRootType().getJdbcId());

            // For domain types, the postgres driver is unable to traverse a nested unbounded
            // hierarchy of types and report the right length/scale of a given type. We use
            // the TypeRegistry to accomplish this since it is capable of traversing the type
            // hierarchy upward to resolve length/scale regardless of hierarchy depth.
            if (TypeRegistry.DOMAIN_TYPE == nativeType.getJdbcId()) {
                column.length(nativeType.getDefaultLength());
                column.scale(nativeType.getDefaultScale());
            }

            final String defaultValueExpression = columnMetadata.getString(13);
            if (defaultValueExpression != null && getDefaultValueConverter().supportConversion(column.typeName())) {
                column.defaultValueExpression(defaultValueExpression);
            }

            return Optional.of(column);
        }

        return Optional.empty();
    }

    public OpengaussDefaultValueConverter getDefaultValueConverter() {
        Objects.requireNonNull(defaultValueConverter, "Connection does not provide default value converter");
        return defaultValueConverter;
    }

    public TypeRegistry getTypeRegistry() {
        Objects.requireNonNull(typeRegistry, "Connection does not provide type registry");
        return typeRegistry;
    }

    @Override
    public <T extends DatabaseSchema<TableId>> Object getColumnValue(ResultSet rs, int columnIndex, Column column,
                                                                     Table table, T schema)
            throws SQLException {
        try {
            final ResultSetMetaData metaData = rs.getMetaData();
            final String columnTypeName = metaData.getColumnTypeName(columnIndex);
            final OpengaussType type = ((OpengaussSchema) schema).getTypeRegistry().get(columnTypeName);

            LOGGER.trace("Type of incoming data is: {}", type.getOid());
            LOGGER.trace("ColumnTypeName is: {}", columnTypeName);
            LOGGER.trace("Type is: {}", type);

            if (type.isArrayType()) {
                return rs.getArray(columnIndex);
            }

            switch (type.getOid()) {
                case OgOid.MONEY:
                    // TODO author=Horia Chiorean date=14/11/2016 description=workaround for https://github.com/pgjdbc/pgjdbc/issues/100
                    final String sMoney = rs.getString(columnIndex);
                    if (sMoney == null) {
                        return sMoney;
                    }
                    if (sMoney.startsWith("-")) {
                        // PGmoney expects negative values to be provided in the format of "($XXXXX.YY)"
                        final String negativeMoney = "(" + sMoney.substring(1) + ")";
                        return new PGmoney(negativeMoney).val;
                    }
                    return new PGmoney(sMoney).val;
                case OgOid.BIT:
                    return rs.getString(columnIndex);
                case OgOid.NUMERIC:
                    final String s = rs.getString(columnIndex);
                    if (s == null) {
                        return s;
                    }

                    Optional<SpecialValueDecimal> value = OpengaussValueConverter.toSpecialValue(s);
                    return value.isPresent() ? value.get() : new SpecialValueDecimal(rs.getBigDecimal(columnIndex));
                case OgOid.TIME:
                    // To handle time 24:00:00 supported by TIME columns, read the column as a string.
                case OgOid.TIMETZ:
                    // In order to guarantee that we resolve TIMETZ columns with proper microsecond precision,
                    // read the column as a string instead and then re-parse inside the converter.
                    return rs.getString(columnIndex);
                default:
                    Object x = rs.getObject(columnIndex);
                    if (x != null) {
                        LOGGER.trace("rs getobject returns class: {}; rs getObject value is: {}", x.getClass(), x);
                    }
                    return x;
            }
        }
        catch (SQLException e) {
            // not a known type
            return super.getColumnValue(rs, columnIndex, column, table, schema);
        }
    }

    @Override
    public synchronized void setSessionParameter() {
        Connection conn = getConnection();
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("set session_timeout = 0");
            stmt.execute("set dolphin.b_compatibility_mode to on");
        } catch (SQLException exp) {
            LOGGER.error("{}SQL Exception occurred when set session parameter.", ErrorCode.DB_CONNECTION_EXCEPTION);
        }
    }

    /**
     * Create a statement for the database session
     *
     * @param connectorConfig opengauss source config
     * @param connection opengauss Connection
     * @return Statement
     * @throws SQLException createStatement exception
     */
    public Statement readTableStatementOpengauss(OpengaussConnectorConfig connectorConfig, Connection connection)
            throws SQLException {
        int fetchSize = connectorConfig.getSnapshotFetchSize();
        final Statement statement = connection.createStatement(); // the default cursor is FORWARD_ONLY
        statement.setFetchSize(fetchSize);
        return statement;
    }

    @FunctionalInterface
    public interface PostgresValueConverterBuilder {
        OpengaussValueConverter build(TypeRegistry registry);
    }
}
