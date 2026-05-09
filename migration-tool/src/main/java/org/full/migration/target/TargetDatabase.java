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

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.full.migration.constants.CommonConstants;
import org.full.migration.coordinator.ProgressTracker;
import org.full.migration.coordinator.QueueManager;
import org.full.migration.enums.SqlCompatibilityEnum;
import org.full.migration.exception.DatabaseConnectionException;
import org.full.migration.exception.ErrorCode;
import org.full.migration.jdbc.JdbcConnection;
import org.full.migration.jdbc.OpenGaussConnection;
import org.full.migration.model.object.DbObject;
import org.full.migration.model.FullName;
import org.full.migration.model.PostgresCustomTypeMeta;
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
import org.full.migration.translator.TranslatorFactory;
import org.full.migration.utils.DatabaseUtils;
import org.full.migration.utils.FileUtils;
import org.opengauss.copy.CopyManager;
import org.opengauss.core.BaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Arrays;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.regex.Pattern;

/**
 * TargetDatabase
 *
 * @since 2025-04-18
 */
@Data
public class TargetDatabase extends AbstractTargetDatabase{
    private static final Logger LOGGER = LoggerFactory.getLogger(TargetDatabase.class);
    private static final Pattern CSV_SPLIT_PATTERN = Pattern.compile(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
    private static final String CREATE_SCHEMA_SQL = "create schema if not exists \"%s\"";
    private static final String DROP_SCHEMA_SQL = "drop schema if exists \"%s\" cascade";
    private static final String DROP_TABLE_SQL = "drop table if exists \"%s\"";
    private static final String COPY_SQL
        = "COPY \"%s\".\"%s\" FROM STDIN WITH NULL 'null' CSV QUOTE '\"' DELIMITER ',' ESCAPE '\"' HEADER";
    private static final String CREATE_PK_SQL = "ALTER TABLE \"%s\".\"%s\" ADD CONSTRAINT \"%s\" PRIMARY KEY (%s)";
    private static final String CREATE_FK_SQL
        = "ALTER TABLE \"%s\".\"%s\" ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES \"%s\".\"%s\" (\"%s\")";
    private static final String IS_TABLE_EXIST_SQL
        = "SELECT EXISTS (SELECT 1  FROM pg_tables  WHERE LOWER(tablename) = ? AND LOWER(schemaname) = ? )";

    /**
     * dbConfig
     */
    protected DatabaseConfig dbConfig;
   
    private BigInteger spacePerSlice;
    private boolean isDeleteCsv;
    private boolean isKeepExistingSchema;
    private Map<String, String> schemaMappings;
    private List<String> createdTables = new ArrayList<>();

    /**
     * Constructor
     *
     * @param globalConfig globalConfig
     * @param targetSqlCompatibility targetSqlCompatibility
     */
    public TargetDatabase(GlobalConfig globalConfig, SqlCompatibilityEnum targetSqlCompatibility) {
        super(globalConfig);
        this.dbConfig = globalConfig.getOgConn();
       
        this.spacePerSlice = globalConfig.getSourceConfig().convertFileSize();
        this.isDeleteCsv = globalConfig.getIsDeleteCsv();
        this.isKeepExistingSchema = globalConfig.getIsKeepExistingSchema();
        this.schemaMappings = globalConfig.getSourceConfig().getSchemaMappings();
        super.connection = new OpenGaussConnection(targetSqlCompatibility);
        initSnapshotRecordTable();
    }

    private void initSnapshotRecordTable() {
        try (Connection connection = this.connection.getConnection(this.dbConfig);
             Statement statement = connection.createStatement();) {
            for (String sql : CommonConstants.SNAPSHOT_TABLE_CREATE_SQL) {
                statement.addBatch(sql);
            }
            statement.executeBatch();
            LOGGER.info("create snapshot record table successfully");
        } catch (SQLException e) {
            LOGGER.error("create snapshot record table occurred error: ", e);
        }
    }

    /**
     * createSchemas
     *
     * @param schemas schemas
     */
    public void createSchemas(Set<String> schemas) {
        for (String schema : schemas) {
            try (Connection conn = connection.getConnection(dbConfig); Statement stmt = conn.createStatement()) {
                String sql = String.format(CREATE_SCHEMA_SQL, schema);
                if (isKeepExistingSchema) {
                    stmt.execute(sql);
                } else {
                    conn.setAutoCommit(false);
                    stmt.execute(String.format(DROP_SCHEMA_SQL, schema));
                    stmt.execute(sql);
                    conn.commit();
                }
                LOGGER.info("finish to create schemas.{}", schema);
            } catch (SQLException e) {
                LOGGER.error("fail to create schema:{}, error message:{}.", schema, e.getMessage());
            }
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
                try {
                    if (isKeepExistingSchema && isTableExist(conn, table)) {
                        continue;
                    }
                    String parents = tableMeta.getParents();
                    List<String> parentTables = parseParents(parents, tableMeta.getTable().getTargetSchemaName());
                    if (canCreateTable(parentTables)) {
                        LOGGER.info("start to migration table:{}", table.getTableName());
                        copyMeta(tableMeta, conn);
                    } else {
                        QueueManager.getInstance()
                                .putToQueue(QueueManager.SOURCE_TABLE_META_QUEUE, tableMeta);
                        continue;
                    }
                } catch (SQLException e) {
                    conn.rollback();
                    LOGGER.error("fail to create table {}.{}, errMsg:{}", table.getTargetSchemaName(),
                        table.getTableName(), e.getMessage());
                }
                QueueManager.getInstance().putToQueue(QueueManager.TARGET_TABLE_META_QUEUE, tableMeta);
            }
            LOGGER.info("{} finished to create table.", Thread.currentThread().getName());
        } catch (SQLException e) {
            LOGGER.warn("Unable to connect to database {}.{}, please check the status of database and config file",
                    dbConfig.getHost(), dbConfig.getDatabase(), e);
        } catch (Exception e) {
            LOGGER.error("Failed to create table", e);
        }
        Thread.currentThread().interrupt();
    }

    /**
     * createCustomOrDomainTypes
     */
    public void createCustomOrDomainTypes(List<PostgresCustomTypeMeta> customTypes) {
        LOGGER.info("start to migration custom types");
        if (CollectionUtils.isEmpty(customTypes)) {
            LOGGER.info("There are no custom types to be migrated.");
            return;
        }
        try (Connection conn = connection.getConnection(dbConfig)) {
            for (PostgresCustomTypeMeta customType : customTypes) {
                String sourceSchema = customType.getSchemaName();
                String sinkSchema = schemaMappings.get(sourceSchema);
                String typeName = customType.getTypeName();
                String createTypeSql = customType.getCreateTypeSql().replace(sourceSchema + ".", sinkSchema + ".");
                LOGGER.info("start to migration custom types:{}.{}", sourceSchema, typeName);
                try (Statement statement = conn.createStatement()) {
                    conn.setAutoCommit(false);
                    statement.execute(createTypeSql);
                    conn.commit();
                    LOGGER.info("create type: {}.{} success", sinkSchema, typeName);
                } catch (SQLException e) {
                    conn.rollback();
                    LOGGER.error("fail to create type {}.{}, errMsg:{}", sinkSchema, typeName, e.getMessage());
                }
            }
            LOGGER.info("finished to migration custom types.");
        } catch (SQLException e) {
            LOGGER.error("Unable to create custom or domain types");
        }
    }

    /**
     * parseParents
     *
     * @param parentsStr
     * @param schema
     * @return parents
     */
    private List<String> parseParents(String parentsStr, String schema) {
        if (StringUtils.isEmpty(parentsStr)) {
            return new ArrayList<>();
        }
        List<String> parents = new ArrayList<>();
        Arrays.asList(parentsStr.split(",")).forEach((parentTable) -> {
            parents.add(schema + "." + parentTable);
        });
        return parents;
    }

    /**
     * canCreateTable
     *
     * @param parentTables
     * @return canCreate
     */
    private boolean canCreateTable(List<String> parentTables) {
        boolean canCreate = true;
        for (String parent : parentTables) {
            if (!createdTables.contains(parent)) {
                canCreate = false;
                break;
            }
        }
        return canCreate;
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
                    insertTableSnapshotInfo(conn, tableData);
                    copyData(tableData, conn);
                } catch (SQLException | IOException e) {
                    Table table = tableData.getTable();
                    LOGGER.warn("fail to write table {}.{}, errMsg:{}", table.getSchemaName(), table.getTableName(),
                            e.getMessage());
                }
            }
            LOGGER.info("{} finished to write table.", Thread.currentThread().getName());
        } catch (Exception e) {
            LOGGER.error("Failed to write table data", e);
        }
        Thread.currentThread().interrupt();
    }

    /**
     * Generates an SQL INSERT statement for the given sink data record.
     * This method is responsible for constructing the SQL statement to insert a record into the replica tables.
     *
     * @param conn
     * @param tableData The sink data record containing the source field information.
     * @return The SQL INSERT statement as a string.
     */
    public void insertTableSnapshotInfo(Connection conn, TableData tableData) {
        String schemaName = tableData.getTable().getSchemaName();
        String tableName = tableData.getTable().getTableName();
        String xlogLocation = tableData.getSnapshotPoint();
        try (Statement stmt = conn.createStatement()) {
            String sql = String.format(CommonConstants.INSERT_REPLICA_TABLES_SQL, schemaName, tableName, xlogLocation, xlogLocation);
            stmt.execute(sql);
            LOGGER.info("{}.{} snapshot information has inserted into sch_debezium.pg_replica_tables.",
                    schemaName, tableName);
        } catch (SQLException e) {
            LOGGER.error("{}.{} snapshot information has failed to insert into sch_debezium.pg_replica_tables, errMsg:{}",
                    schemaName, tableName, e.getMessage());
        }
    }

    private void copyData(TableData tableTask, Connection connection) throws SQLException, IOException {
        String schemaName = tableTask.getTable().getSchemaName();
        String tableName = tableTask.getTable().getTableName();
        String fullName = (new FullName(schemaName, tableName).getFullName());
        String path = tableTask.getDataPath();
        ProgressInfo progressInfo = new ProgressInfo();
        progressInfo.setSchema(schemaName);
        progressInfo.setName(tableName);
        if (StringUtils.isEmpty(path) && !tableTask.getTable().isPartition()) {
            if (isJsonDump) {
                progressInfo.setPercent(1);
                progressInfo.setStatus(ProgressStatus.MIGRATED_COMPLETE.getCode());
                ProgressTracker.getInstance().upgradeTableProgress(fullName, progressInfo);
            }
            LOGGER.debug("{}.{} is an empty table, no need to copy data.", schemaName, tableName);
            return;
        }
        String targetSchema = tableTask.getTable().getTargetSchemaName();
        connection.setSchema(targetSchema);
        SliceInfo sliceInfo;
        CopyManager copyManager = new CopyManager((BaseConnection) connection);
        try (InputStreamReader csvReader = new InputStreamReader(Files.newInputStream(Paths.get(path)),
            StandardCharsets.UTF_8)) {
            String copySql = String.format(COPY_SQL, targetSchema, tableName);
            copyManager.copyIn(copySql, csvReader);
            csvReader.close();
            FileUtils.clearCsvFile(path, isDeleteCsv);
            sliceInfo = tableTask.getSliceInfo();
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
            sliceInfo = tableTask.getSliceInfo();
            progressInfo.setData(calculateProgressData(sliceInfo));
            progressInfo.setRecord(sliceInfo.getRow());
            boolean isSuccess = copyLineByLine(copyManager, String.format("%s.%s", targetSchema, tableName), path);
            if (!isSuccess) {
                progressInfo.setStatus(ProgressStatus.MIGRATED_FAILURE.getCode());
                progressInfo.setPercent(ProgressStatus.MIGRATED_FAILURE.getCode());
                progressInfo.setError(e.getMessage());
                LOGGER.error("failed to copy data of {}.{}, error message:{}", schemaName, tableName, e.getMessage());
            }
        } finally {
            if (isJsonDump) {
                sliceInfo = tableTask.getSliceInfo();
                if (sliceInfo.isLast()) {
                    progressInfo.setPercent(1);
                    progressInfo.setStatus(ProgressStatus.MIGRATED_COMPLETE.getCode());
                }
                ProgressTracker.getInstance().upgradeTableProgress(fullName, progressInfo);
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

    private boolean copyLineByLine(CopyManager copyManager, String fullName, String csvPath)
            throws SQLException, IOException {
        StringBuilder failedLines = new StringBuilder();
        String header;
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(csvPath, StandardCharsets.UTF_8))) {
            header = bufferedReader.readLine();
            if (header == null) {
                throw new IOException("CSV file has no header");
            }

            int fieldNum = header.split(",").length;
            StringBuilder fieldBuilder = new StringBuilder();
            List<String> rowFields = new ArrayList<>();
            AtomicBoolean inQuotes = new AtomicBoolean(false);

            String csvLine;
            String tableRow = "";
            boolean isParseSuccess;
            while ((csvLine = readLine(bufferedReader)) != null) {
                isParseSuccess = parseLine(rowFields, fieldBuilder, fieldNum, csvLine, inQuotes);
                if (!isParseSuccess) {
                    failedLines.append(tableRow).append(csvLine);
                } else if (inQuotes.get()) {
                    tableRow += csvLine;
                    continue;
                } else {
                    copyLine(copyManager, buildCopySql(fullName, header), rowFields, failedLines);
                }

                fieldBuilder.setLength(0);
                rowFields = new ArrayList<>();
                inQuotes.set(false);
                tableRow = "";
            }
        }
        if (!failedLines.isEmpty()) {
            writeFailSqlCsv(Collections.singletonList(failedLines.toString()), header, csvPath);
            return false;
        }
        return true;
    }

    private void copyLine(CopyManager copyManager, String copySql, List<String> fields, StringBuilder failedLines) {
        String line = buildCsvLine(fields);
        try (StringReader stringReader = new StringReader(line)) {
            copyManager.copyIn(copySql, stringReader);
        } catch (SQLException | IOException e) {
            LOGGER.error("Failed to copy data to table, error: {}", e.getMessage());
            failedLines.append(line).append("\n");
        }
    }

    private String buildCsvLine(List<String> fields) {
        StringBuilder stringBuilder = new StringBuilder();
        for (String field : fields) {
            if (field == null) {
                stringBuilder.append("null").append(CommonConstants.DELIMITER);
            } else {
                stringBuilder.append("\"")
                        .append(field.replaceAll("\"", "\"\""))
                        .append("\"")
                        .append(CommonConstants.DELIMITER);
            }
        }
        return stringBuilder.substring(0, stringBuilder.length() - 1);
    }

    private String buildCopySql(String fullName, String header) {
        return String.format(
                "COPY %s (%s) FROM STDIN WITH (FORMAT csv, NULL 'null', QUOTE '\"', DELIMITER ',', ESCAPE '\"')",
                fullName, header
        );
    }

    private boolean parseLine(List<String> rowFields, StringBuilder fieldBuilder, int fieldNum,
                              String line, AtomicBoolean atomicInQuotes) {
        String endCRLF = getEndCRLF(line);
        String lineWithoutCRLF = line.substring(0, line.length() - endCRLF.length());

        char c;
        boolean inQuotes = atomicInQuotes.get();
        for (int i = 0; i < lineWithoutCRLF.length(); i++) {
            c = lineWithoutCRLF.charAt(i);
            if (c == '"') {
                // handle quote escaping: two consecutive double quotes represent a single double quote character
                if (inQuotes && i + 1 < lineWithoutCRLF.length() && lineWithoutCRLF.charAt(i + 1) == '"') {
                    fieldBuilder.append('"');
                    i++;
                } else {
                    // switch the inQuotes status
                    inQuotes = !inQuotes;
                    atomicInQuotes.set(inQuotes);

                    // handle end of line
                    if (!inQuotes && i == lineWithoutCRLF.length() - 1) {
                        addField(rowFields, fieldBuilder);
                    }
                }
            } else if (c == ',' && !inQuotes) {
                addField(rowFields, fieldBuilder);
            } else if (inQuotes) {
                fieldBuilder.append(c);
            } else if (lineWithoutCRLF.startsWith("null,", i) || lineWithoutCRLF.endsWith("null")) {
                // handle null value
                rowFields.add(null);
                i += 4;
            } else {
                return false;
            }
        }

        if (inQuotes) {
            if (rowFields.size() < fieldNum) {
                fieldBuilder.append(endCRLF);
                return true;
            }
        } else {
            return rowFields.size() == fieldNum;
        }
        return false;
    }

    private void addField(List<String> rowFields, StringBuilder fieldBuilder) {
        rowFields.add(fieldBuilder.toString());
        fieldBuilder.setLength(0);
    }

    private String getEndCRLF(String line) {
        if (line.endsWith("\r\n")) {
            return "\r\n";
        } else if (line.endsWith("\n")) {
            return "\n";
        } else {
            return "";
        }
    }

    private String readLine(BufferedReader bufferedReader) throws IOException {
        StringBuilder line = new StringBuilder();
        int ch;
        char c;
        while ((ch = bufferedReader.read()) != -1) {
            c = (char) ch;
            line.append(c);

            if (c == '\n') {
                break;
            }
        }
        return line.isEmpty() ? null : line.toString();
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

    /**
     * writeObjects
     *
     * @param objectType objectType
     */
    public void writeObjects(String sourceDbType, String objectType) {
        try (Connection conn = connection.getConnection(dbConfig); Statement statement = conn.createStatement()) {
            while (!QueueManager.getInstance().isQueuePollEnd(QueueManager.OBJECT_QUEUE)) {
                DbObject object = (DbObject) QueueManager.getInstance().pollQueue(QueueManager.OBJECT_QUEUE);
                if (object == null) {
                    continue;
                }
                String sourceSchema = object.getSchema();
                String targetSchema = schemaMappings.get(sourceSchema);
                FullName sourceFullName = new FullName(sourceSchema, object.getName());
                String createObjectSql = object.getDefinition();
                if (createObjectSql.contains(sourceSchema + ".")) {
                    createObjectSql = createObjectSql.replace(sourceSchema + ".", targetSchema + ".");
                }
                try {
                    executeCreateObject(sourceDbType, objectType, targetSchema, conn, statement, createObjectSql, sourceFullName);
                } catch (SQLException e) {
                    LOGGER.warn(
                            "Method 1 directly execute create {} {} has occurred an exception, detail:{}, so translate it"
                                    + " according to sql-translator.", objectType, sourceFullName.getFullName(), e.getMessage());
                    Optional<String> translatedSql = TranslatorFactory.translate(sourceDbType,
                            createObjectSql, false, false);
                    if (!translatedSql.isPresent()) {
                        handleCreateFailure(objectType, sourceFullName, "sql-translator failed");
                    } else {
                        try {
                            executeCreateObject(sourceDbType, objectType, targetSchema, conn, statement, translatedSql.get(), sourceFullName);
                        } catch (SQLException ex) {
                            handleCreateFailure(objectType, sourceFullName, e.getMessage());
                        }
                    }
                }
            }
            LOGGER.info("{} has finished to write {}.", Thread.currentThread().getName(), objectType);
        } catch (SQLException e) {
            LOGGER.warn("write {} has occurred an exception, detail:{}", objectType, e.getMessage());
        }
    }

    private void executeCreateObject(String sourceDbType, String objectType, String sinkSchema, Connection conn,
                                     Statement statement, String sqlStr, FullName sourceFullName) throws SQLException {
        try {
            conn.setAutoCommit(false);
            conn.setSchema(sinkSchema);

            if (sqlStr.contains(";") &&
                    !((sourceDbType.equalsIgnoreCase("postgresql") ||
                            sourceDbType.equalsIgnoreCase("opengauss")) &&
                            (objectType.equalsIgnoreCase("function") ||
                                    objectType.equalsIgnoreCase("procedure")))) {
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
                        .upgradeObjectProgressMap(sourceFullName, ProgressStatus.MIGRATED_COMPLETE, StringUtils.EMPTY);
            }
        } catch (SQLException e) {
            conn.rollback();
            throw e;
        }
    }

    private void handleCreateFailure(String objectType, FullName fullName, String errMsg) {
        LOGGER.error("Method 2 execute failed to create {} {}. detail:{}.", objectType, fullName.getFullName(), errMsg);
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

   

    private Optional<String> getCreateIndexSql(TableIndex tableIndex) {
        Optional<String> indexSqlTempOptional = getIndexSqlTemp(tableIndex);
        if (indexSqlTempOptional.isPresent()) {
            StringBuilder builder;
            if (!tableIndex.isConstraint()) {
                builder = new StringBuilder(
                        String.format(indexSqlTempOptional.get(),
                                DatabaseUtils.formatObjName(tableIndex.getIndexName()),
                                DatabaseUtils.formatObjName(tableIndex.getSchemaName()),
                                DatabaseUtils.formatObjName(tableIndex.getTableName()),
                                StringUtils.isEmpty(tableIndex.getIndexprs()) ? tableIndex.getColumnName() : tableIndex.getIndexprs()));
            } else {
                builder = new StringBuilder(
                        String.format(indexSqlTempOptional.get(),
                                DatabaseUtils.formatObjName(tableIndex.getSchemaName()),
                                DatabaseUtils.formatObjName(tableIndex.getTableName()),
                                DatabaseUtils.formatObjName(tableIndex.getIndexName()),
                                StringUtils.isEmpty(tableIndex.getIndexprs()) ? tableIndex.getColumnName() : tableIndex.getIndexprs()));
            }
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
        if ("CLUSTERED".equals(indexType) || "NONCLUSTERED".equals(indexType) || "BTREE".equals(indexType)) {
            if (tableIndex.isUnique()) {
                createIndexTemp = tableIndex.isConstraint()
                        ? "alter table %s.%s add constraint %s unique(%s)"
                        : "CREATE UNIQUE INDEX %s ON %s.%s USING btree (%s)";
            } else {
                createIndexTemp = "CREATE INDEX %s ON %s.%s USING btree (%s)";
            }
        } else if ("FULLTEXT".equals(indexType) || "XML".equalsIgnoreCase(indexType)) {
            createIndexTemp = "CREATE INDEX %s ON %s.%s USING gin (%s gin_trgm_ops)";
        } else if ("GIST".equals(indexType) || "SPATIAL".equals(indexType)) {
            createIndexTemp = "CREATE INDEX %s ON %s.%s USING gist (%s)";
        } else {
            createIndexTemp = "CREATE INDEX %s ON %s.%s (%s)";
        }
        String indexRange = tableIndex.getIndexRange() == null
                ? null
                : tableIndex.getIndexRange().toUpperCase(Locale.ROOT);
        if (!tableIndex.isConstraint()) {
            if ("LOCAL".equals(indexRange)) {
                createIndexTemp += " LOCAL";
            } else if ("GLOBAL".equals(indexRange)) {
                createIndexTemp += " GLOBAL";
            }
        }
        return Optional.of(createIndexTemp);
    }

    private Optional<String> getCreatePkSql(TablePrimaryKey tablePrimaryKey) {
        return Optional.of(String.format(CREATE_PK_SQL, tablePrimaryKey.getSchemaName(), tablePrimaryKey.getTableName(),
                tablePrimaryKey.getPkName(), DatabaseUtils.formatMultiColName(tablePrimaryKey.getColumnName())));
    }

    private Optional<String> getCreateFkSql(TableForeignKey tableForeignKey) {
        return Optional.of(
            String.format(CREATE_FK_SQL, tableForeignKey.getSchemaName(), tableForeignKey.getParentTable(),
                tableForeignKey.getFkName(), tableForeignKey.getParentColumn(), tableForeignKey.getSchemaName(),
                tableForeignKey.getReferencedTable(), tableForeignKey.getReferencedColumn()));
    }

    /**
     * writeObjects
     */
    public void dropReplicaSchema() {
        try (Connection conn = connection.getConnection(dbConfig); Statement statement = conn.createStatement()) {
            statement.execute(CommonConstants.DROP_REPLICA_SCHEMA_SQL);
            LOGGER.info("drop replica schema(sch_debezium) success.");
        } catch (SQLException e) {
            LOGGER.error("drop replica schema(sch_debezium) has occurred an exception, detail:{}", e.getMessage());
        }
    }
}
