/*
 * Copyright (c) 2025-2025 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *           http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.full.migration.source;

import lombok.Data;

import org.apache.commons.lang3.StringUtils;
import org.full.migration.constants.CommonConstants;
import org.full.migration.coordinator.ProgressTracker;
import org.full.migration.coordinator.QueueManager;
import org.full.migration.jdbc.JdbcConnection;
import org.full.migration.model.DbObject;
import org.full.migration.model.PostgresCustomTypeMeta;
import org.full.migration.model.TaskTypeEnum;
import org.full.migration.model.config.GlobalConfig;
import org.full.migration.model.config.SourceConfig;
import org.full.migration.model.table.Column;
import org.full.migration.model.table.GenerateInfo;
import org.full.migration.model.table.SliceInfo;
import org.full.migration.model.table.Table;
import org.full.migration.model.table.TableData;
import org.full.migration.model.table.TableForeignKey;
import org.full.migration.model.table.TableIndex;
import org.full.migration.model.table.TableMeta;
import org.full.migration.model.table.TablePrimaryKey;
import org.full.migration.source.service.SourceTableService;
import org.full.migration.utils.FileUtils;
import org.full.migration.utils.HexConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.ArrayList;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * SourceDatabase
 *
 * @since 2025-04-18
 */
@Data
public abstract class SourceDatabase {
    private static final Logger LOGGER = LoggerFactory.getLogger(SourceDatabase.class);
    private static final Integer EMPTY_TABLE_PAGE_ROWS = 100000;

    /**
     * connection
     */
    protected JdbcConnection connection;

    /**
     * sourceConfig
     */
    protected SourceConfig sourceConfig;

    private final SourceTableService sourceTableService;
    private final boolean isDumpJson;

    /**
     * Constructor
     *
     * @param globalConfig globalConfig
     */
    public SourceDatabase(GlobalConfig globalConfig) {
        this.sourceConfig = globalConfig.getSourceConfig();
        this.sourceTableService = new SourceTableService(sourceConfig);
        this.isDumpJson = globalConfig.getIsDumpJson();
    }

    /**
     * getQueryTableSql
     *
     * @param schema schema
     * @return sql for querying Table
     */
    protected abstract List<Table> getSchemaAllTables(String schema, Connection conn);

    /**
     * createCustomOrDomainTypesSql
     *
     * @param conn
     * @param schema
     * @return postgresCustomTypeMetas
     */
    protected abstract List<PostgresCustomTypeMeta> createCustomOrDomainTypesSql(Connection conn, String schema) ;

    /**
     * getGeneratedDefine
     *
     * @param conn conn
     * @param schema schema
     * @param tableName tableName
     * @param name name
     * @return generated column
     */
    protected abstract Optional<GenerateInfo> getGeneratedDefine(Connection conn, String schema, String tableName,
        String name);

    /**
     * getQueryUniqueConstraint
     *
     * @return sql for querying UniqueConstraint
     */
    protected abstract String getQueryUniqueConstraint();

    /**
     * getQueryCheckConstraint
     *
     * @return sql for querying checkConstraint
     */
    protected abstract String getQueryCheckConstraint();

    public abstract String convertToOpenGaussSyntax(String sqlServerDefinition);

    public abstract boolean isGeometryTypes(String typeName);

    /**
     * IsColumnGenerate
     *
     * @param conn
     * @param schema
     * @param tableName
     * @param column
     * @return is column generate
     */
    protected abstract boolean IsColumnGenerate(Connection conn, String schema, String tableName, Column column);

    /**
     * getPartitionDdl
     *
     * @param conn conn,schema schema, tableName tableName
     * @return partitionDdl
     */
    public abstract String getPartitionDdl(Connection conn, String schema, String tableName, boolean isSubPartition) throws SQLException;

    /**
     * getPartitionDdl
     *
     * @param conn conn,schema schema, tableName tableName
     * @return partitionDdl
     */
    public abstract String getParentTables(Connection conn, Table table) throws SQLException;

    /**
     * isPartitionChildTable
     *
     * @param schema
     * @param table
     * @param connection
     * @return isPartitionChildTable
     */
    public abstract boolean isPartitionChildTable(String schema, String table, Connection connection);

    /**
     *
     * @param table
     * @param columns
     * @return columnDdl
     */
    public abstract String getColumnDdl(Table table, List<Column> columns);

    /**
     * getIsolationSql
     *
     * @return IsolationSql
     */
    protected abstract String getIsolationSql();

    /**
     * lockTable
     *
     * @param table
     * @param conn
     * @throws SQLException
     */
    protected abstract void lockTable(Table table, Connection conn) throws SQLException;

    /**
     * getQueryWithLock
     *
     * @param table table
     * @param columns columns
     * @return sql for querying table
     */
    protected abstract String getQueryWithLock(Table table, List<Column> columns, Connection conn);

    /**
     * getSnapShotPoint
     *
     * @return sql for querying snapShot point
     */
    protected abstract String getSnapShotPoint(Connection conn);

    /**
     * getQueryObjectSql
     *
     * @param objectType objectType
     * @return sql for querying object point
     * @throws IllegalArgumentException IllegalArgumentException
     */
    protected abstract String getQueryObjectSql(String objectType, Connection connection) throws IllegalArgumentException;

    /**
     * convertDefinition
     *
     * @param objectType objectType
     * @param rs rs
     * @return definition
     * @throws SQLException SQLException
     */
    protected abstract String convertDefinition(String objectType, ResultSet rs) throws SQLException;

    /**
     * getQueryIndexSql
     *
     * @param schema schema
     * @return sql for querying index
     */
    protected abstract String getQueryIndexSql(String schema);

    /**
     * getTableIndex
     *
     * @param conn conn
     * @param rs rs
     * @return tableIndex
     * @throws SQLException SQLException
     */
    protected abstract TableIndex getTableIndex(Connection conn, ResultSet rs) throws SQLException;

    /**
     * getQueryPkSql
     *
     * @return sql for querying primary key
     */
    protected abstract String getQueryPkSql();

    /**
     * getQueryFkSql
     *
     * @param schema schema
     * @return sql for querying foreign key
     */
    protected abstract String getQueryFkSql(String schema);

    /**
     * createSourceLogicalReplicationSlot
     *
     * @param conn
     */
    public abstract void createSourceLogicalReplicationSlot(Connection conn);

    /**
     * dropSourceLogicalReplicationSlot
     *
     * @param conn
     */
    public abstract void dropSourceLogicalReplicationSlot(Connection conn);

    /**
     * setReplicaIdentity
     *
     * @param table
     */
    protected abstract void setReplicaIdentity(Table table);

    /**
     * initPublication
     *
     * @param conn
     * @param migraTableNames
     */
    protected abstract void initPublication(Connection conn, List<String> migraTableNames);

    /**
     * getSchemaSet
     *
     * @return Set<String>
     */
    public Set<String> getSchemaSet() {
        return sourceConfig.getSchemaMappings().keySet();
    }

    /**
     * isNotNeedMigraTable
     * The tables that are configured to be skipped do not need to be migrated.
     * And the child tables of the partitioned tables in Postgres are independent tables. No migration is required in OpenGauss.
     *
     * @param schema schema
     * @param tableName tableName
     * @param conn conn
     * @return isSkipTable
     */
    private boolean isNotNeedMigraTable(String schema, String tableName, Connection conn) {
        boolean isSkipTable = sourceTableService.isSkipTable(schema, tableName);
        if (isSkipTable || isPartitionChildTable(schema, tableName, conn)) {
            return true;
        }
        return false;
    }

    /**
     * initializeLogicalReplication
     *
     * @param schemaSet
     */
    public void initializeLogicalReplication(Set<String> schemaSet) {
        if (!sourceConfig.getIsRecordSnapshot()) {
            return;
        }
        try (Connection conn = connection.getConnection(sourceConfig.getDbConn())) {
            List<String> migraTableNames = new ArrayList<>();
            for (String schema : schemaSet) {
                List<Table> tables = getSchemaAllTables(schema, conn);
                for (Table table : tables) {
                    if (isNotNeedMigraTable(schema, table.getTableName(), conn)) {
                        continue;
                    }
                    migraTableNames.add(table.getSchemaName() + "." + table.getTableName());
                    setReplicaIdentity(table);
                }
            }
            initPublication(conn, migraTableNames);
            dropSourceLogicalReplicationSlot(conn);
            createSourceLogicalReplicationSlot(conn);
        } catch (SQLException e) {
            LOGGER.warn("fail to create logical replication slot, error message:{}.", e.getMessage());
        }

    }

    /**
     * queryCustomOrDomainTypes
     *
     * @param schemaSet
     * @return postgresCustomTypeMetas
     */
    public List<PostgresCustomTypeMeta> queryCustomOrDomainTypes(Set<String> schemaSet) {
        List<PostgresCustomTypeMeta> postgresCustomTypeMetas = new ArrayList<>();
        try (Connection conn = connection.getConnection(sourceConfig.getDbConn());
             Statement stmt = conn.createStatement()) {
            for (String schema : schemaSet) {
                postgresCustomTypeMetas.addAll(createCustomOrDomainTypesSql(conn, schema));
            }
            return postgresCustomTypeMetas;
        } catch (SQLException e) {
            LOGGER.error("fail to query table list, error message:{}.", e.getMessage());
        }
        return postgresCustomTypeMetas;
    }

    /**
     * queryTables
     *
     * @param schemaSet schemaSet
     */
    public void queryTables(Set<String> schemaSet) {
        try (Connection conn = connection.getConnection(sourceConfig.getDbConn());
            Statement stmt = conn.createStatement()) {
            for (String schema : schemaSet) {
                List<Table> tables = getSchemaAllTables(schema, conn);
                for (Table table : tables) {
                    String tableName = table.getTableName();
                    if (isNotNeedMigraTable(schema, tableName, conn)) {
                        continue;
                    }
                    QueueManager.getInstance().putToQueue(QueueManager.TABLE_QUEUE, table);
                    if (isDumpJson) {
                        ProgressTracker.getInstance().putProgressMap(schema, tableName);
                    }
                }
            }
            if (isDumpJson) {
                ProgressTracker.getInstance().recordTableProgress();
            }
            LOGGER.info("success to query all tables...");
        } catch (SQLException e) {
            LOGGER.error("fail to query table list, error message:{}.", e.getMessage());
        }
        QueueManager.getInstance().setReadFinished(QueueManager.TABLE_QUEUE, true);
        Thread.currentThread().interrupt();
    }

    /**
     * readTableConstruct
     */
    public void readTableConstruct() {
        try (Connection conn = connection.getConnection(sourceConfig.getDbConn())) {
            while (!QueueManager.getInstance().isQueuePollEnd(QueueManager.TABLE_QUEUE)) {
                Table table = (Table) QueueManager.getInstance().pollQueue(QueueManager.TABLE_QUEUE);
                if (table == null) {
                    continue;
                }
                String schema = table.getSchemaName();
                String tableName = table.getTableName();
                LOGGER.info("start to read metadata of {}.{}.", schema, tableName);
                DatabaseMetaData metaData = conn.getMetaData();
                ResultSet columnMetadata = metaData.getColumns(table.getCatalogName(), schema, tableName, null);
                List<Column> columns = new ArrayList<>();
                while (columnMetadata.next()) {
                    sourceTableService.readTableColumn(columnMetadata).ifPresent(column -> {
                        Optional<GenerateInfo> generateInfoOptional = getGeneratedDefine(conn, schema, tableName,
                            column.getName());
                        if (IsColumnGenerate(conn, schema, tableName, column) && generateInfoOptional.isPresent()) {
                            column.setGenerated(true);
                            column.setGenerateInfo(generateInfoOptional.get());
                        }
                        columns.add(column);
                    });
                }
                // 构造建表语句
                String partitionDdl = null;
                if (table.isPartition()) {
                    partitionDdl = getPartitionDdl(conn, schema, tableName, table.isSubPartition());
                }
                String inheritsDdl = null;
                String parents = getParentTables(conn, table);
                if (!(StringUtils.isEmpty(parents))) {
                    inheritsDdl = String.format(" Inherits (%s)", parents);
                }

                String columnsDdl = getColumnDdl(table, columns);

                Optional<String> createTableSqlOptional = sourceTableService.getCreateTableSql(table, columnsDdl,
                    partitionDdl, inheritsDdl);
                if (!createTableSqlOptional.isPresent()) {
                    continue;
                }
                QueueManager.getInstance()
                    .putToQueue(QueueManager.SOURCE_TABLE_META_QUEUE,
                        new TableMeta(table, createTableSqlOptional.get(), columns, parents));
            }
        } catch (SQLException e) {
            LOGGER.error(
                "Unable to connect to database {}.{}, error message:{}, please check the status of database and "
                    + "config file", sourceConfig.getDbConn().getHost(), sourceConfig.getDbConn().getDatabase(),
                e.getMessage());
        }
    }

    /**
     * readTable
     */
    public void readTable() {
        try (Connection conn = connection.getConnection(sourceConfig.getDbConn())) {
            while (!QueueManager.getInstance().isQueuePollEnd(QueueManager.TARGET_TABLE_META_QUEUE)) {
                TableMeta tableMeta = (TableMeta) QueueManager.getInstance()
                    .pollQueue(QueueManager.TARGET_TABLE_META_QUEUE);
                if (tableMeta == null) {
                    continue;
                }
                Table table = tableMeta.getTable();
                LOGGER.info("start to read {}.{}.", table.getSchemaName(), table.getTableName());
                try {
                    if (table.getRowCount() == 0) {
                        QueueManager.getInstance()
                            .putToQueue(QueueManager.TABLE_DATA_QUEUE,
                                new TableData(table, StringUtils.EMPTY, StringUtils.EMPTY, new SliceInfo()));
                        continue;
                    }
                    extractData(table, tableMeta.getColumns(), conn);
                } catch (SQLException | InterruptedException | IOException e) {
                    LOGGER.error("read table:{}.{} has occurred an exception, error message:{}", table.getSchemaName(),
                        table.getTableName(), e.getMessage());
                }
            }
            LOGGER.info("{} finished to read table.", Thread.currentThread().getName());
        } catch (SQLException e) {
            LOGGER.error(
                "Unable to connect to database {}.{}, error message:{}, please check the status of database and "
                    + "config file", sourceConfig.getDbConn().getHost(), sourceConfig.getDbConn().getDatabase(),
                e.getMessage());
        }
        Thread.currentThread().interrupt();
    }

    private void extractData(Table table, List<Column> columns, Connection conn)
        throws SQLException, IOException, InterruptedException {
        conn.setAutoCommit(false);
        lockTable(table, conn);
        String snapshotPoint = null;
        if (sourceConfig.getIsRecordSnapshot()) {
            snapshotPoint = recordSnapshotPoint(conn);
        }
        int pageRows = Math.max(table.getAveRowLength() == 0
            ? EMPTY_TABLE_PAGE_ROWS
            : (int) (sourceConfig.convertFileSize().intValue() / (table.getAveRowLength())), 1);
        LOGGER.info("start to export data for table {}.{}", table.getSchemaName(), table.getTableName());
        try (Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            stmt.setFetchSize(pageRows);
            List<Column> queryColumns = columns.stream()
                .filter(column -> !column.isGenerated())
                .collect(Collectors.toList());
            String lockQuery = getQueryWithLock(table, queryColumns, conn);
            try (ResultSet rs = stmt.executeQuery(lockQuery)) {
                exportResultSetToCsv(rs, table, queryColumns, pageRows, snapshotPoint);
            }
        }
        conn.commit();
    }



    /**
     * exportResultSetToCsv
     *
     * @param rs rs
     * @param table table
     * @param columns columns
     * @param pageRows pageRows
     * @param snapshotPoint snapshotPoint
     * @throws SQLException SQLException
     * @throws IOException IOException
     */
    public void exportResultSetToCsv(ResultSet rs, Table table, List<Column> columns, int pageRows,
                                     String snapshotPoint) throws SQLException, IOException {
        String tableCsvPath = sourceConfig.getCsvDir();
        FileUtils.createDir(tableCsvPath);
        int fileIndex = 1;
        BufferedWriter writer = initializeWriter(table, tableCsvPath, fileIndex, columns);
        try {
            int rowCount = 0;
            long totalSlice = table.getRowCount() / pageRows + 1;
            while (rs.next()) {
                rowCount++;
                String line = formatData(rs, columns);
                writer.write(line);
                writer.newLine();
                if (rowCount % pageRows == 0) {
                    writer.flush();
                    writer.close();
                    SliceInfo sliceInfo = new SliceInfo(fileIndex, totalSlice, pageRows, false);
                    TableData tableData = new TableData(table,
                            FileUtils.getCurrentFilePath(table, tableCsvPath, fileIndex), snapshotPoint, sliceInfo);
                    updateTableDataQueue(tableData);
                    fileIndex++;
                    writer = initializeWriter(table, tableCsvPath, fileIndex, columns);
                    rowCount = 0;
                }
            }
            TableData tableData = new TableData(table, FileUtils.getCurrentFilePath(table, tableCsvPath, fileIndex),
                    snapshotPoint, new SliceInfo(fileIndex, totalSlice, rowCount, true));
            if (rowCount == 0) {
                String filePath = FileUtils.getCurrentFilePath(table, tableCsvPath, fileIndex - 1);
                SliceInfo sliceInfo = new SliceInfo(fileIndex - 1, totalSlice, pageRows, true);
                tableData.setDataPath(filePath);
                tableData.setSliceInfo(sliceInfo);
                updateTableDataQueue(tableData);
            } else {
                writer.flush();
                writer.close();
                updateTableDataQueue(tableData);
            }
        } catch (IOException e) {
            LOGGER.error("write csv file has occurred an IOException, error message:{}", e.getMessage());
        } finally {
            if (writer != null) {
                writer.close();
            }
        }
        LOGGER.info("finished to read table:{}, generate {} csv files", table.getTableName(), fileIndex);
    }

    private void updateTableDataQueue(TableData tableData) {
        QueueManager.getInstance().putToQueue(QueueManager.TABLE_DATA_QUEUE, tableData);
    }

    private BufferedWriter initializeWriter(Table table, String tableCsvPath, int fileIndex, List<Column> columns) {
        BufferedWriter writer = FileUtils.createNewFileWriter(table, tableCsvPath, fileIndex);
        String header = formatHeader(columns);
        try {
            writer.write(header);
            writer.newLine();
        } catch (IOException e) {
            LOGGER.error("Failed to initialize writer. error messages: {}", e.getMessage());
        }
        return writer;
    }

    private String formatHeader(List<Column> columns) {
        return columns.stream().map(Column::getName).collect(Collectors.joining(CommonConstants.DELIMITER));
    }

    private String formatData(ResultSet rs, List<Column> columns) throws SQLException {
        int columnCount = columns.size();
        final List<Object> rowList = new ArrayList<>(columnCount);
        for (int i = 0; i < columnCount; i++) {
            String typeName = columns.get(i).getTypeName();
            Object value = getColumnValue(rs, i + 1, typeName);
            if (isGeometryTypes(typeName)) {
                if (value.toString().toLowerCase(Locale.ROOT).contains("point")) {
                    value = convertToOpenGaussSyntax(value.toString());
                }
            }
            rowList.add(value);
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < columnCount; i++) {
            Object value = rowList.get(i);
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
                sb.append("\"")
                        .append(value.toString().replace("\"", "\"\""))
                        .append("\"")
                        .append(CommonConstants.DELIMITER);
            } else {
                sb.append(value).append(CommonConstants.DELIMITER);
            }
            if (i == columnCount - 1) {
                sb.deleteCharAt(sb.length() - 1);
            }
        }
        return sb.toString();
    }

    private Object getColumnValue(ResultSet rs, int columnIndex, String typeName) throws SQLException {
        // getObject 对time类型截断小数位，所使用getString读取该类型
        // getObject 对timestamp和data类型的‘infinity’值会进行内部处理导致value值异常
        if ("time".equalsIgnoreCase(typeName) || "date".equalsIgnoreCase(typeName) || "timestamp".equalsIgnoreCase(typeName)) {
            return rs.getString(columnIndex);
        } else if ("money".equalsIgnoreCase(typeName)) {
            //读取的money值会有逗号分隔数值，插入时会失败
            return handleMoneyType(rs, columnIndex);
        } else {
            return rs.getObject(columnIndex);
        }
    }

    private Object handleMoneyType(ResultSet rs, int columnIndex) throws SQLException {
        try {
            BigDecimal decimalValue = rs.getBigDecimal(columnIndex);
            return decimalValue != null ? decimalValue.toPlainString() : null;
        } catch (SQLException e) {
            try {
                String moneyStr = rs.getString(columnIndex);
                return moneyStr != null ? moneyStr.replace(",", "") : null;
            } catch (SQLException ex) {
                Object rawValue = rs.getObject(columnIndex);
                return rawValue != null ? rawValue.toString().replace(",", "") : null;
            }
        }
    }

    private String recordSnapshotPoint(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(getSnapShotPoint(conn))) {
            if (rs.next()) {
                return rs.getString(1);
            }
        }
        return null;
    }

    /**
     * readObjects
     *
     * @param objectType objectType
     * @param schema schema
     */
    public void readObjects(String sourceDbType, String objectType, String schema) {
        try (Connection conn = connection.getConnection(sourceConfig.getDbConn());
            Statement statement = conn.createStatement()) {
            String querySql = String.format(getQueryObjectSql(objectType.toLowerCase(Locale.ROOT), conn), schema);
            ResultSet rs = statement.executeQuery(querySql);
            while (rs.next()) {
                DbObject dbObject = new DbObject();
                String objectName = rs.getString("name");
                dbObject.setSchema(schema);
                dbObject.setName(objectName);
                dbObject.setDefinition(convertDefinition(objectType, rs));
                LOGGER.debug("read object, type:{}, object Name:{}", objectType, dbObject.getName());
                QueueManager.getInstance().putToQueue(QueueManager.OBJECT_QUEUE, dbObject);
                if (isDumpJson) {
                    ProgressTracker.getInstance().putProgressMap(schema, objectName);
                }
            }
            if (isDumpJson) {
                ProgressTracker.getInstance().recordObjectProgress(TaskTypeEnum.getTaskTypeEnum(objectType));
            }
        } catch (SQLException e) {
            LOGGER.error("fail to read {}, errorMsg:{}", objectType, e.getMessage());
        }
        QueueManager.getInstance().setReadFinished(QueueManager.OBJECT_QUEUE, true);
    }

    /**
     * readTableIndex
     *
     * @param schemaSet schemaSet
     */
    public void readTableIndex(Set<String> schemaSet) {
        try (Connection conn = connection.getConnection(sourceConfig.getDbConn());
            Statement stmt = conn.createStatement()) {
            for (String schema : schemaSet) {
                try (ResultSet rs = stmt.executeQuery(getQueryIndexSql(schema))) {
                    while (rs.next()) {
                        TableIndex tableIndex = getTableIndex(conn, rs);
                        if (isNotNeedMigraTable(schema, tableIndex.getTableName(), conn)) {
                            continue;
                        }
                        tableIndex.setSchemaName(sourceConfig.getSchemaMappings().get(schema));
                        if (!tableIndex.isPrimaryKey() && !tableIndex.isUnique()) {
                            QueueManager.getInstance().putToQueue(QueueManager.TABLE_INDEX_QUEUE, tableIndex);
                        }
                        if (isDumpJson) {
                            ProgressTracker.getInstance().putProgressMap(schema, tableIndex.getIndexName());
                        }
                    }
                }
                if (isDumpJson) {
                    ProgressTracker.getInstance().recordKeyAndIndexProgress(TaskTypeEnum.INDEX);
                }
            }
        } catch (SQLException e) {
            LOGGER.error("fail to read indexes, errorMsg:{}", e.getMessage());
        }
        QueueManager.getInstance().setReadFinished(QueueManager.TABLE_INDEX_QUEUE, true);
        LOGGER.info("end to read table indexes.");
    }

    /**
     * readTablePk
     *
     * @param schemaSet schemaSet
     */
    public void readTablePk(Set<String> schemaSet) {
        try (Connection conn = connection.getConnection(sourceConfig.getDbConn())) {
            for (String schema : schemaSet) {
                try (PreparedStatement pstmt = conn.prepareStatement(getQueryPkSql())) {
                    pstmt.setString(1, schema);
                    try (ResultSet rs = pstmt.executeQuery()) {
                        while (rs.next()) {
                            TablePrimaryKey tablePrimaryKey = new TablePrimaryKey();
                            tablePrimaryKey.setSchemaName(sourceConfig.getSchemaMappings().get(schema));
                            tablePrimaryKey.setTableName(rs.getString("table_name"));
                            tablePrimaryKey.setColumnName(rs.getString("pk_columns"));
                            tablePrimaryKey.setPkName(rs.getString("pk_name"));
                            if (isNotNeedMigraTable(schema, tablePrimaryKey.getTableName(), conn)) {
                                continue;
                            }
                            QueueManager.getInstance()
                                .putToQueue(QueueManager.TABLE_PRIMARY_KEY_QUEUE, tablePrimaryKey);
                            if (isDumpJson) {
                                ProgressTracker.getInstance().putProgressMap(schema, tablePrimaryKey.getPkName());
                            }
                        }
                    }
                }
                if (isDumpJson) {
                    ProgressTracker.getInstance().recordKeyAndIndexProgress(TaskTypeEnum.PRIMARY_KEY);
                }
            }
        } catch (SQLException e) {
            LOGGER.error("fail to read table primary keys, errorMsg:{}", e.getMessage());
        }
        QueueManager.getInstance().setReadFinished(QueueManager.TABLE_PRIMARY_KEY_QUEUE, true);
        LOGGER.info("end to read table primary keys.");
    }

    /**
     * readTableFk
     *
     * @param schemaSet schemaSet
     */
    public void readTableFk(Set<String> schemaSet) {
        try (Connection conn = connection.getConnection(sourceConfig.getDbConn());
            Statement stmt = conn.createStatement()) {
            for (String schema : schemaSet) {
                ResultSet rs = stmt.executeQuery(getQueryFkSql(schema));
                while (rs.next()) {
                    TableForeignKey tableForeignKey = new TableForeignKey(rs);
                    tableForeignKey.setSchemaName(sourceConfig.getSchemaMappings().get(schema));
                    QueueManager.getInstance().putToQueue(QueueManager.TABLE_FOREIGN_KEY_QUEUE, tableForeignKey);
                    if (isDumpJson) {
                        ProgressTracker.getInstance().putProgressMap(schema, tableForeignKey.getFkName());
                    }
                }
                if (isDumpJson) {
                    ProgressTracker.getInstance().recordKeyAndIndexProgress(TaskTypeEnum.FOREIGN_KEY);
                }
            }
        } catch (SQLException e) {
            LOGGER.error("fail to read views, errorMsg:{}", e.getMessage());
        }
        QueueManager.getInstance().setReadFinished(QueueManager.TABLE_FOREIGN_KEY_QUEUE, true);
        LOGGER.info("end to read table foreign keys.");
    }

    /**
     * readConstraints,including unique constraints and check constraints
     *
     * @param schemaSet schemaSet
     */
    public void readConstraints(Set<String> schemaSet) {
        try (Connection conn = connection.getConnection(sourceConfig.getDbConn())) {
            for (String schema : schemaSet) {
                processSchemaConstraints(conn, schema);
            }
        } catch (SQLException e) {
            LOGGER.error("fail to read table constraints, errorMsg:{}", e.getMessage());
        }
        QueueManager.getInstance().setReadFinished(QueueManager.TABLE_CONSTRAINT_QUEUE, true);
        LOGGER.info("end to read table constraints.");
    }

    private void processSchemaConstraints(Connection conn, String schema) throws SQLException {
        String targetSchema = sourceConfig.getSchemaMappings().get(schema);
        processConstraints(conn, schema, getQueryUniqueConstraint(),
            (tableName, constraintName, columns) -> buildUniqueConstraintSql(targetSchema, tableName, constraintName,
                columns), "unique");
        processConstraints(conn, schema, getQueryCheckConstraint(), (tableName, constraintName, definition) -> {
            String convertedDef = convertToOpenGaussSyntax(definition);
            return buildCheckConstraintSql(targetSchema, tableName, constraintName, convertedDef);
        }, "check");
    }

    private void processConstraints(Connection conn, String schema, String query, ConstraintProcessor processor,
        String constraintType) throws SQLException {
        try (PreparedStatement pstmt = conn.prepareStatement(query)) {
            pstmt.setString(1, schema);
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    String tableName = rs.getString("table_name");
                    if (sourceTableService.isSkipTable(schema, tableName)) {
                        continue;
                    }
                    String constraintName = rs.getString("constraint_name");
                    String constraintValue = constraintType.equals("unique")
                        ? rs.getString("columns")
                        : rs.getString("definition");
                    String sql = processor.process(tableName, constraintName, constraintValue);
                    QueueManager.getInstance().putToQueue(QueueManager.TABLE_CONSTRAINT_QUEUE, sql);
                }
            }
        }
    }

    @FunctionalInterface
    private interface ConstraintProcessor {
        String process(String tableName, String constraintName, String constraintValue);
    }

    private String buildUniqueConstraintSql(String schemaName, String tableName, String constraintName,
        String uniqueColumns) {
        String columns = String.join(", ", uniqueColumns.split(","));
        return String.format("ALTER TABLE %s.%s ADD CONSTRAINT \"%s\" UNIQUE (%s)", schemaName, tableName,
            constraintName, columns);
    }

    private String buildCheckConstraintSql(String schema, String tableName, String constraintName, String definition) {
        return String.format("ALTER TABLE %s.%s ADD CONSTRAINT %s CHECK (%s)", schema, tableName, constraintName,
            definition);
    }
}
