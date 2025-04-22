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
import org.full.migration.coordinator.ProgressTracker;
import org.full.migration.coordinator.QueueManager;
import org.full.migration.jdbc.JdbcConnection;
import org.full.migration.model.DbObject;
import org.full.migration.model.TaskTypeEnum;
import org.full.migration.model.config.GlobalConfig;
import org.full.migration.model.config.SourceConfig;
import org.full.migration.model.table.Column;
import org.full.migration.model.table.PartitionDefinition;
import org.full.migration.model.table.SliceInfo;
import org.full.migration.model.table.Table;
import org.full.migration.model.table.TableData;
import org.full.migration.model.table.TableForeignKey;
import org.full.migration.model.table.TableIndex;
import org.full.migration.model.table.TableMeta;
import org.full.migration.model.table.TablePrimaryKey;
import org.full.migration.source.service.SourceTableService;
import org.full.migration.translator.SqlServerFuncTranslator;
import org.full.migration.utils.HexConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

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
    protected abstract String getQueryTableSql(String schema);

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

    /**
     * getPartitionDefinition
     *
     * @param conn conn
     * @param schemaName schemaName
     * @param tableName tableName
     * @return PartitionDefinition
     * @throws SQLException SQLException
     */
    protected abstract PartitionDefinition getPartitionDefinition(Connection conn, String schemaName, String tableName)
        throws SQLException;

    /**
     * getIsolationSql
     *
     * @return IsolationSql
     */
    protected abstract String getIsolationSql();

    /**
     * getQueryWithLock
     *
     * @param table table
     * @param columns columns
     * @return sql for querying table
     */
    protected abstract String getQueryWithLock(Table table, List<Column> columns);

    /**
     * getSnapShotPoint
     *
     * @return sql for querying snapShot point
     */
    protected abstract String getSnapShotPoint();

    /**
     * getQueryObjectSql
     *
     * @param objectType objectType
     * @return sql for querying object point
     * @throws IllegalArgumentException IllegalArgumentException
     */
    protected abstract String getQueryObjectSql(String objectType) throws IllegalArgumentException;

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
     * getSchemaSet
     *
     * @return Set<String>
     */
    public Set<String> getSchemaSet() {
        return sourceConfig.getSchemaMappings().keySet();
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
                String queryTableSql = getQueryTableSql(schema);
                try (ResultSet rs = stmt.executeQuery(queryTableSql)) {
                    while (rs.next()) {
                        String tableName = rs.getString("TableName");
                        if (sourceTableService.isSkipTable(schema, tableName)) {
                            continue;
                        }
                        Table table = new Table(sourceConfig.getDbConn().getDatabase(), schema, tableName);
                        table.setTargetSchemaName(sourceConfig.getSchemaMappings().get(schema));
                        table.setAveRowLength(rs.getLong("avgRowLength"));
                        table.setRowCount(rs.getInt("tableRows"));
                        table.setPartition(rs.getBoolean("isPartitioned"));
                        QueueManager.getInstance().putToQueue(QueueManager.TABLE_QUEUE, table);
                        if (isDumpJson) {
                            ProgressTracker.getInstance().putProgressMap(schema, tableName);
                        }
                    }
                }
            }
            if (isDumpJson) {
                ProgressTracker.getInstance().recordTableProgress();
            }
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
                LOGGER.info("start to read metadata of {}.{}.", table.getSchemaName(), table.getTableName());
                DatabaseMetaData metaData = conn.getMetaData();
                ResultSet columnMetadata = metaData.getColumns(table.getCatalogName(), table.getSchemaName(),
                    table.getTableName(), null);
                List<Column> columns = new ArrayList<>();
                while (columnMetadata.next()) {
                    sourceTableService.readTableColumn(columnMetadata).ifPresent(columns::add);
                }
                // 构造建表语句
                String partitionDdl = null;
                if (table.isPartition()) {
                    PartitionDefinition partitionDef = getPartitionDefinition(conn, table.getSchemaName(),
                        table.getTableName());
                    partitionDdl = sourceTableService.getPartitionDdl(partitionDef);
                }
                Optional<String> createTableSqlOptional = sourceTableService.getCreateTableSql(table, columns,
                    partitionDdl);
                if (!createTableSqlOptional.isPresent()) {
                    continue;
                }
                List<String> uniqueSqlList = getUniqueSqlList(conn, table);
                List<String> checkSqlList = getCheckSqlList(conn, table);
                QueueManager.getInstance()
                    .putToQueue(QueueManager.SOURCE_TABLE_META_QUEUE,
                        new TableMeta(table, createTableSqlOptional.get(), columns, uniqueSqlList, checkSqlList));
            }
        } catch (SQLException e) {
            LOGGER.error(
                "Unable to connect to database {}.{}, error message:{}, please check the status of database and "
                    + "config file", sourceConfig.getDbConn().getHost(), sourceConfig.getDbConn().getDatabase(),
                e.getMessage());
        }
    }

    private List<String> getCheckSqlList(Connection conn, Table table) throws SQLException {
        String queryCheckConstraintSql = getQueryCheckConstraint();
        List<String> checkSqlList = new ArrayList<>();
        try (PreparedStatement pstmt = conn.prepareStatement(queryCheckConstraintSql)) {
            pstmt.setString(1, table.getTableName());
            pstmt.setString(2, table.getSchemaName());
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    String constraintName = rs.getString("constraint_name");
                    String definition = SqlServerFuncTranslator.convertDefinition(rs.getString("definition"));
                    checkSqlList.add(
                        buildCheckConstraintSql(table.getTargetSchemaName(), table.getTableName(), constraintName,
                            definition));
                }
            }
        }
        return checkSqlList;
    }

    private List<String> getUniqueSqlList(Connection conn, Table table) throws SQLException {
        List<String> uniqueSqlList = new ArrayList<>();
        try (PreparedStatement pstmt = conn.prepareStatement(getQueryUniqueConstraint())) {
            pstmt.setString(1, table.getTableName());
            pstmt.setString(2, table.getSchemaName());
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    String constraintName = rs.getString("constraint_name");
                    String uniqueColumns = rs.getString("columns");
                    uniqueSqlList.add(
                        buildUniqueConstraintSql(table.getTargetSchemaName(), table.getTableName(), constraintName,
                            uniqueColumns));
                }
            }
        }
        return uniqueSqlList;
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
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(getIsolationSql());
        }
        String snapshotPoint = null;
        if (sourceConfig.getIsRecordSnapshot()) {
            byte[] snapshotPointByte = recordSnapshotPoint(conn);
            snapshotPoint = HexConverter.convertToHexString(snapshotPointByte);
        }
        int pageRows = Math.max(table.getAveRowLength() == 0
            ? EMPTY_TABLE_PAGE_ROWS
            : (int) (sourceConfig.convertFileSize().intValue() / table.getAveRowLength()), 1);
        LOGGER.info("start to export data for table {}.{}", table.getSchemaName(), table.getTableName());
        try (Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            stmt.setFetchSize(pageRows);
            String lockQuery = getQueryWithLock(table, columns);
            try (ResultSet rs = stmt.executeQuery(lockQuery)) {
                sourceTableService.exportResultSetToCsv(rs, table, columns, pageRows, snapshotPoint);
            }
        }
        conn.commit();
    }

    private byte[] recordSnapshotPoint(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(getSnapShotPoint())) {
            if (rs.next()) {
                return rs.getBytes("max_lsn");
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
    public void readObjects(String objectType, String schema) {
        try (Connection conn = connection.getConnection(sourceConfig.getDbConn());
            Statement statement = conn.createStatement()) {
            String querySql = String.format(getQueryObjectSql(objectType.toLowerCase(Locale.ROOT)), schema);
            ResultSet rs = statement.executeQuery(querySql);
            while (rs.next()) {
                DbObject dbObject = new DbObject();
                String objectName = rs.getString("name");
                dbObject.setSchema(schema);
                dbObject.setName(objectName);
                dbObject.setDefinition(convertDefinition(objectType, rs));
                LOGGER.info("{}:{}", objectType, dbObject.getDefinition());
                QueueManager.getInstance().putToQueue(QueueManager.OBJECT_QUEUE, dbObject);
                if (isDumpJson) {
                    ProgressTracker.getInstance().putProgressMap(schema, objectName);
                }
            }
            if (isDumpJson) {
                ProgressTracker.getInstance().recordObjectProgress(TaskTypeEnum.valueOf(objectType));
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
                        tableIndex.setSchemaName(sourceConfig.getSchemaMappings().get(schema));
                        if (!(tableIndex.isPrimaryKey() && tableIndex.isUnique())) {
                            QueueManager.getInstance().putToQueue(QueueManager.TABLE_INDEX_QUEUE, tableIndex);
                        }
                    }
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
                            QueueManager.getInstance()
                                .putToQueue(QueueManager.TABLE_PRIMARY_KEY_QUEUE, tablePrimaryKey);
                        }
                    }
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
                }
            }
        } catch (SQLException e) {
            LOGGER.error("fail to read views, errorMsg:{}", e.getMessage());
        }
        QueueManager.getInstance().setReadFinished(QueueManager.TABLE_FOREIGN_KEY_QUEUE, true);
        LOGGER.info("end to read table foreign keys.");
    }
}
