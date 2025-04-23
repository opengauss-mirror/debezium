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

package org.full.migration.target;

import lombok.Data;

import org.apache.commons.lang3.StringUtils;
import org.full.migration.constants.CommonConstants;
import org.full.migration.coordinator.ProgressTracker;
import org.full.migration.coordinator.QueueManager;
import org.full.migration.jdbc.JdbcConnection;
import org.full.migration.jdbc.OpenGaussConnection;
import org.full.migration.model.DbObject;
import org.full.migration.model.config.DatabaseConfig;
import org.full.migration.model.config.GlobalConfig;
import org.full.migration.model.progress.ProgressInfo;
import org.full.migration.model.progress.ProgressStatus;
import org.full.migration.model.table.SliceInfo;
import org.full.migration.model.table.Table;
import org.full.migration.model.table.TableData;
import org.full.migration.model.table.TableForeignKey;
import org.full.migration.model.table.TableIndex;
import org.full.migration.model.table.TableMeta;
import org.full.migration.model.table.TablePrimaryKey;
import org.full.migration.translator.SqlServer2OpenGaussTranslator;
import org.full.migration.utils.FileUtils;
import org.opengauss.copy.CopyManager;
import org.opengauss.core.BaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Pattern;

/**
 * TargetDatabase
 *
 * @since 2025-04-18
 */
@Data
public class TargetDatabase {
    private static final Logger LOGGER = LoggerFactory.getLogger(TargetDatabase.class);
    private static final Pattern CSV_SPLIT_PATTERN = Pattern.compile(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
    private static final String CREATE_SCHEMA_SQL = "create schema if not exists %s";
    private static final String DROP_SCHEMA_SQL = "drop schema if exists %s cascade";
    private static final String DROP_TABLE_SQL = "drop table if exists %s";
    private static final String COPY_SQL
        = "COPY %s.%s FROM STDIN WITH NULL 'null' CSV QUOTE '\"' DELIMITER ',' ESCAPE '\"' HEADER";
    private static final String CREATE_PK_SQL = "ALTER TABLE %s.%s ADD CONSTRAINT %s PRIMARY KEY (%s)";
    private static final String CREATE_FK_SQL
        = "ALTER TABLE %s.%s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s.%s (%s)";
    private static final String IS_TABLE_EXIST_SQL
        = "SELECT EXISTS (SELECT 1  FROM pg_tables  WHERE LOWER(tablename) = ? AND LOWER(schemaname) = ? )";

    /**
     * dbConfig
     */
    protected DatabaseConfig dbConfig;

    /**
     * connection
     */
    protected JdbcConnection connection;
    private boolean isJsonDump;
    private BigInteger spacePerSlice;
    private boolean isDeleteCsv;
    private boolean isKeepExistingSchema;
    private Map<String, String> schemaMappings;

    /**
     * Constructor
     *
     * @param globalConfig globalConfig
     */
    public TargetDatabase(GlobalConfig globalConfig) {
        this.dbConfig = globalConfig.getOgConn();
        this.isJsonDump = globalConfig.getIsDumpJson();
        this.spacePerSlice = globalConfig.getSourceConfig()
            .initStoreSize(globalConfig.getSourceConfig().getFileSize())
            .divide(BigInteger.valueOf(1024 * 1024));
        this.isDeleteCsv = globalConfig.getIsDeleteCsv();
        this.isKeepExistingSchema = globalConfig.getIsKeepExistingSchema();
        this.schemaMappings = globalConfig.getSourceConfig().getSchemaMappings();
        this.connection = new OpenGaussConnection();
    }

    /**
     * createSchemas
     *
     * @param schemas schemas
     */
    public void createSchemas(Set<String> schemas) {
        try (Connection conn = connection.getConnection(dbConfig); Statement stmt = conn.createStatement()) {
            for (String schema : schemas) {
                String sql = String.format(CREATE_SCHEMA_SQL, schema);
                if (isKeepExistingSchema) {
                    stmt.execute(sql);
                } else {
                    conn.setAutoCommit(false);
                    stmt.execute(String.format(DROP_SCHEMA_SQL, schema));
                    stmt.execute(sql);
                    conn.commit();
                }
            }
            LOGGER.info("finish to create schemas.");
        } catch (SQLException e) {
            LOGGER.error("fail to create schema:{}, error message:{}.", schemas, e.getMessage());
        }
    }

    /**
     * writeTableConstruct
     */
    public void writeTableConstruct() {
        try (Connection conn = connection.getConnection(dbConfig)) {
            while (!QueueManager.getInstance().isQueuePollEnd(QueueManager.SOURCE_TABLE_META_QUEUE)) {
                TableMeta tableMeta = (TableMeta) QueueManager.getInstance()
                    .pollQueue(QueueManager.SOURCE_TABLE_META_QUEUE);
                if (tableMeta == null) {
                    continue;
                }
                Table table = tableMeta.getTable();
                LOGGER.info("start to migration table:{}", table.getTableName());
                try {
                    if (isKeepExistingSchema && isTableExist(conn, table)) {
                        continue;
                    }
                    copyMeta(tableMeta, conn);
                } catch (SQLException e) {
                    conn.rollback();
                    LOGGER.error("fail to create table {}.{}, errMsg:{}", table.getTargetSchemaName(),
                        table.getTableName(), e.getMessage());
                }
                QueueManager.getInstance().putToQueue(QueueManager.TARGET_TABLE_META_QUEUE, tableMeta);
            }
            LOGGER.info("{} finished to create table.", Thread.currentThread().getName());
        } catch (SQLException e) {
            LOGGER.warn(
                "Unable to connect to database {}.{}, error message:{}, please check the status of database and "
                    + "config file", dbConfig.getHost(), dbConfig.getDatabase(), e.getMessage());
        }
        Thread.currentThread().interrupt();
    }

    private boolean isTableExist(Connection conn, Table table) throws SQLException {
        try (PreparedStatement pstmt = conn.prepareStatement(IS_TABLE_EXIST_SQL)) {
            pstmt.setString(1, table.getTableName().toLowerCase(Locale.ROOT));
            pstmt.setString(2, table.getTargetSchemaName().toLowerCase(Locale.ROOT));
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getBoolean("exists");
                }
            }
        }
        return false;
    }

    /**
     * writeTable
     */
    public void writeTable() {
        try (Connection conn = connection.getConnection(dbConfig)) {
            while (!QueueManager.getInstance().isQueuePollEnd(QueueManager.TABLE_DATA_QUEUE)) {
                TableData tableData = (TableData) QueueManager.getInstance().pollQueue(QueueManager.TABLE_DATA_QUEUE);
                if (tableData == null) {
                    continue;
                }
                LOGGER.debug("start to migration table:{}.", tableData.getTable().getTableName());
                try {
                    copyData(tableData, conn);
                } catch (SQLException | IOException e) {
                    Table table = tableData.getTable();
                    LOGGER.warn("fail to write table {}.{}, errMsg:{}", table.getSchemaName(), table.getTableName(),
                        e.getMessage());
                }
            }
            LOGGER.info("{} finished to write table.", Thread.currentThread().getName());
        } catch (SQLException e) {
            LOGGER.warn("fail to write table, errMsg:{}", e.getMessage());
        }
        Thread.currentThread().interrupt();
    }

    private void copyMeta(TableMeta tableMeta, Connection conn) throws SQLException {
        try (Statement statement = conn.createStatement()) {
            Table table = tableMeta.getTable();
            conn.setAutoCommit(false);
            conn.setSchema(table.getTargetSchemaName());
            statement.execute(String.format(DROP_SCHEMA_SQL, table.getTableName()));
            statement.execute(tableMeta.getCreateTableSql());
            conn.commit();
            executeAlterTable(conn, statement, tableMeta.getCheckSqlList());
            executeAlterTable(conn, statement, tableMeta.getUniqueSqlList());
            LOGGER.info("create {}.{} success", table.getTargetSchemaName(), table.getTableName());
        }
    }

    private void executeAlterTable(Connection connection, Statement statement, List<String> alterSqlList)
        throws SQLException {
        if (!CollectionUtils.isEmpty(alterSqlList)) {
            for (String alterSql : alterSqlList) {
                try {
                    connection.setAutoCommit(false);
                    statement.execute(alterSql);
                    connection.commit();
                } catch (SQLException e) {
                    connection.rollback();
                    LOGGER.error("failed to create constraint, sql:{}", alterSql);
                }
            }
        }
    }

    private void copyData(TableData tableTask, Connection connection) throws SQLException, IOException {
        String schemaName = tableTask.getTable().getSchemaName();
        String tableName = tableTask.getTable().getTableName();
        String name = String.format("%s.%s", schemaName, tableName);
        String path = tableTask.getDataPath();
        ProgressInfo progressInfo = new ProgressInfo();
        progressInfo.setName(name);
        if (StringUtils.isEmpty(path)) {
            if (isJsonDump) {
                progressInfo.setPercent(1);
                progressInfo.setStatus(ProgressStatus.MIGRATED_COMPLETE.getCode());
                ProgressTracker.getInstance().upgradeTableProgress(name, progressInfo);
            }
            LOGGER.debug("{}.{} is an empty table, no need to copy data.", schemaName, tableName);
            return;
        }
        String targetSchema = tableTask.getTable().getTargetSchemaName();
        connection.setSchema(targetSchema);
        try (FileReader csvReader = new FileReader(path)) {
            CopyManager copyManager = new CopyManager((BaseConnection) connection);
            String copySql = String.format(COPY_SQL, targetSchema, tableName);
            copyManager.copyIn(copySql, csvReader);
            csvReader.close();
            FileUtils.clearCsvFile(path, isDeleteCsv);
            SliceInfo sliceInfo = tableTask.getSliceInfo();
            progressInfo.setData(calculateProgressData(sliceInfo));
            progressInfo.setRecord(sliceInfo.getRow());
            if (sliceInfo.isLast()) {
                progressInfo.setPercent(1);
                progressInfo.setStatus(ProgressStatus.MIGRATED_COMPLETE.getCode());
            } else {
                progressInfo.setPercent(calculateProgressPercent(sliceInfo));
                progressInfo.setStatus(ProgressStatus.IN_MIGRATED.getCode());
            }
        } catch (SQLException e) {
            SliceInfo sliceInfo = tableTask.getSliceInfo();
            progressInfo.setData(calculateProgressData(sliceInfo));
            progressInfo.setRecord(sliceInfo.getRow());
            progressInfo.setStatus(ProgressStatus.MIGRATED_FAILURE.getCode());
            progressInfo.setPercent(ProgressStatus.MIGRATED_FAILURE.getCode());
            progressInfo.setError(e.getMessage());
            LOGGER.error("failed to copy data of {}.{}, error message:{}", schemaName, tableName, e.getMessage());
            insertToTable(connection, String.format("%s.%s", targetSchema, tableName), path);
        } finally {
            if (isJsonDump) {
                ProgressTracker.getInstance().upgradeTableProgress(name, progressInfo);
            }
        }
    }

    private float calculateProgressPercent(SliceInfo sliceInfo) {
        return BigDecimal.valueOf(((float) sliceInfo.getCurrentSlice() / sliceInfo.getTotalSlice()))
            .setScale(2, RoundingMode.HALF_UP)
            .floatValue();
    }

    private double calculateProgressData(SliceInfo sliceInfo) {
        return new BigDecimal(spacePerSlice.multiply(BigInteger.valueOf(sliceInfo.getCurrentSlice()))).setScale(2,
            RoundingMode.HALF_UP).doubleValue();
    }

    private void insertToTable(Connection conn, String fullTableName, String csvFile) throws SQLException, IOException {
        List<String> allLines = Files.readAllLines(Paths.get(csvFile));
        if (allLines.isEmpty()) {
            throw new IOException(csvFile + "is empty, please check...");
        }
        String headers = allLines.get(0);
        List<String> dataLines = new ArrayList<>(allLines.subList(1, allLines.size()));
        List<String> failedLines = new ArrayList<>();
        Iterator<String> iterator = dataLines.iterator();
        String insertSql = buildInsertSql(fullTableName, headers.split(CommonConstants.DELIMITER));
        try (PreparedStatement pstmt = conn.prepareStatement(insertSql)) {
            while (iterator.hasNext()) {
                String line = iterator.next();
                try {
                    String[] values = CSV_SPLIT_PATTERN.split(line, -1);
                    for (int i = 0; i < values.length; i++) {
                        String value = values[i].substring(1, values[i].length() - 1).trim();
                        pstmt.setObject(i + 1, "null".equalsIgnoreCase(value) ? "" : value);
                    }
                    pstmt.executeUpdate();
                } catch (SQLException e) {
                    failedLines.add(line);
                }
            }
        }
        // 失败sql重新写入文件
        writeFailSqlCsv(failedLines, headers, csvFile);
    }

    private void writeFailSqlCsv(List<String> failedLines, String header, String csvFile) throws IOException {
        if (failedLines.isEmpty()) {
            return;
        }
        List<String> newContent = new ArrayList<>();
        newContent.add(header);
        newContent.addAll(failedLines);
        Files.write(Paths.get(csvFile), newContent, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
    }

    private String buildInsertSql(String tableName, String[] columns) {
        String cols = String.join(", ", columns);
        String placeholders = String.join(", ", Collections.nCopies(columns.length, "?"));
        return String.format("INSERT INTO %s (%s) VALUES (%s)", tableName, cols, placeholders);
    }

    /**
     * writeObjects
     *
     * @param objectType objectType
     * @param schema schema
     */
    public void writeObjects(String objectType, String schema) {
        try (Connection conn = connection.getConnection(dbConfig); Statement statement = conn.createStatement()) {
            while (!QueueManager.getInstance().isQueuePollEnd(QueueManager.OBJECT_QUEUE)) {
                DbObject object = (DbObject) QueueManager.getInstance().pollQueue(QueueManager.OBJECT_QUEUE);
                if (object == null) {
                    continue;
                }
                String targetSchema = schemaMappings.get(schema);
                String fullName = String.format("%s.%s", schema, object.getName());
                String createObjectSql = object.getDefinition();
                try {
                    executeCreateObject(targetSchema, conn, statement, createObjectSql, fullName);
                } catch (SQLException e) {
                    LOGGER.warn(
                        "Method 1 directly execute create {} {} has occurred an exception, detail:{}, so translate it"
                            + " according to sql-translator.", objectType, fullName, e.getMessage());
                    Optional<String> translatedSql = SqlServer2OpenGaussTranslator.translateSQLServer2openGauss(
                        createObjectSql, false, false);
                    if (!translatedSql.isPresent()) {
                        handleCreateFailure(objectType, fullName, "sql-translator failed");
                    } else {
                        try {
                            executeCreateObject(targetSchema, conn, statement, translatedSql.get(), fullName);
                        } catch (SQLException ex) {
                            handleCreateFailure(objectType, fullName, e.getMessage());
                        }
                    }
                }
            }
            LOGGER.info("{} has finished to write {}.", Thread.currentThread().getName(), objectType);
        } catch (SQLException e) {
            LOGGER.warn("write {} has occurred an exception, detail:{}", objectType, e.getMessage());
        }
    }

    private void executeCreateObject(String schema, Connection conn, Statement statement, String sqlStr,
        String fullName) throws SQLException {
        try {
            conn.setAutoCommit(false);
            conn.setSchema(schema);
            if (sqlStr.contains(";")) {
                String[] sqls = sqlStr.split(";");
                for (String sql : sqls) {
                    statement.execute(sql);
                }
            } else {
                statement.executeUpdate(sqlStr);
            }
            conn.commit();
            if (isJsonDump) {
                ProgressTracker.getInstance()
                    .upgradeObjectProgressMap(fullName, ProgressStatus.MIGRATED_COMPLETE, StringUtils.EMPTY);
            }
        } catch (SQLException e) {
            conn.rollback();
            throw e;
        }
    }

    private void handleCreateFailure(String objectType, String fullName, String errMsg) {
        LOGGER.error("Method 2 execute failed to create {} {}. detail:{}.", objectType, fullName, errMsg);
        if (isJsonDump) {
            ProgressTracker.getInstance().upgradeObjectProgressMap(fullName, ProgressStatus.MIGRATED_FAILURE, errMsg);
        }
    }

    /**
     * writeTableIndex
     */
    public void writeTableIndex() {
        writeKeyOrIndex(object -> getCreateIndexSql((TableIndex) object), QueueManager.TABLE_INDEX_QUEUE,
            "table index");
    }

    /**
     * writeTablePk
     */
    public void writeTablePk() {
        writeKeyOrIndex(object -> getCreatePkSql((TablePrimaryKey) object), QueueManager.TABLE_PRIMARY_KEY_QUEUE,
            "table primary key");
    }

    /**
     * writeTableFk
     */
    public void writeTableFk() {
        writeKeyOrIndex(object -> getCreateFkSql((TableForeignKey) object), QueueManager.TABLE_FOREIGN_KEY_QUEUE,
            "table foreign key");
    }

    /**
     * writeKeyOrIndex
     *
     * @param sqlGenerator sqlGenerator
     * @param queueName queueName
     * @param logPrefix logPrefix
     */
    public void writeKeyOrIndex(Function<Object, Optional<String>> sqlGenerator, String queueName, String logPrefix) {
        try (Connection conn = connection.getConnection(dbConfig); Statement statement = conn.createStatement()) {
            while (!QueueManager.getInstance().isQueuePollEnd(queueName)) {
                Object object = QueueManager.getInstance().pollQueue(queueName);
                if (object == null) {
                    LOGGER.debug("{} poll from queue is null, to write {}.", Thread.currentThread().getName(),
                        logPrefix);
                    continue;
                }
                try {
                    String sql = sqlGenerator.apply(object)
                        .orElseThrow(() -> new SQLException("This object is not currently supported."));
                    statement.executeUpdate(sql);
                } catch (SQLException e) {
                    LOGGER.error("write {} has occurred an exception,  detail:{}", logPrefix, e.getMessage());
                    continue;
                }
                LOGGER.info("{} has finished to write {}", Thread.currentThread().getName(), logPrefix);
            }
        } catch (SQLException e) {
            LOGGER.warn("Initial connection error while writing {}, detail: {}", logPrefix, e.getMessage());
        }
    }

    private Optional<String> getCreateIndexSql(TableIndex tableIndex) {
        Optional<String> indexSqlTempOptional = getIndexSqlTemp(tableIndex);
        if (indexSqlTempOptional.isPresent()) {
            StringBuilder builder = new StringBuilder(
                String.format(indexSqlTempOptional.get(), tableIndex.getIndexName(), tableIndex.getSchemaName(),
                    tableIndex.getTableName(), tableIndex.getColumnName()));
            if (StringUtils.isNotEmpty(tableIndex.getIncludedColumns())) {
                builder.append(" INCLUDE (").append(tableIndex.getIncludedColumns()).append(")");
            }
            if (tableIndex.isHasFilter() && StringUtils.isNotEmpty(tableIndex.getFilterDefinition())) {
                builder.append(" WHERE ").append(tableIndex.getFilterDefinition());
            }
            return Optional.of(builder.toString());
        }
        return Optional.empty();
    }

    private Optional<String> getIndexSqlTemp(TableIndex tableIndex) {
        String indexType = tableIndex.getIndexType().toUpperCase(Locale.ROOT);
        String createIndexTemp;
        if (indexType.contains("COLUMNSTORE")) {
            LOGGER.error("this type of index is not be supported to migration, schema:{}, table:{}, name:{}. type:{}",
                tableIndex.getSchemaName(), tableIndex.getTableName(), tableIndex.getIndexName(),
                tableIndex.getIndexType());
            return Optional.empty();
        }
        if ("CLUSTERED".equals(indexType) || "NONCLUSTERED".equals(indexType)) {
            createIndexTemp = tableIndex.isUnique()
                ? "CREATE UNIQUE INDEX %s ON %s.%s USING btree (%s)"
                : "CREATE INDEX %s ON %s.%s USING btree (%s)";
        } else if ("FULLTEXT".equals(indexType) || "XML".equalsIgnoreCase(indexType)) {
            createIndexTemp = "CREATE INDEX %s ON %s.%s USING gin (%s gin_trgm_ops)";
        } else if ("SPATIAL".equals(indexType)) {
            createIndexTemp = "CREATE INDEX %s ON %s.%s USING gist (%s)";
        } else {
            createIndexTemp = "CREATE INDEX %s ON %s.%s (%s)";
        }
        return Optional.of(createIndexTemp);
    }

    private Optional<String> getCreatePkSql(TablePrimaryKey tablePrimaryKey) {
        return Optional.of(String.format(CREATE_PK_SQL, tablePrimaryKey.getSchemaName(), tablePrimaryKey.getTableName(),
            tablePrimaryKey.getPkName(), tablePrimaryKey.getColumnName()));
    }

    private Optional<String> getCreateFkSql(TableForeignKey tableForeignKey) {
        return Optional.of(
            String.format(CREATE_FK_SQL, tableForeignKey.getSchemaName(), tableForeignKey.getParentTable(),
                tableForeignKey.getFkName(), tableForeignKey.getParentColumn(), tableForeignKey.getSchemaName(),
                tableForeignKey.getReferencedTable(), tableForeignKey.getReferencedColumn()));
    }
}
