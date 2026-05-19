/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

package org.full.migration.target;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.full.migration.coordinator.ProgressTracker;
import org.full.migration.coordinator.QueueManager;
import org.full.migration.datax.DataXManager;
import org.full.migration.datax.config.DataXConfigContext;
import org.full.migration.exception.ErrorCode;
import org.full.migration.exception.MigrationException;
import org.full.migration.jdbc.OgracConnection;
import org.full.migration.model.FullName;
import org.full.migration.model.PostgresCustomTypeMeta;
import org.full.migration.model.config.GlobalConfig;
import org.full.migration.model.config.SourceConfig;
import org.full.migration.model.object.DbObject;
import org.full.migration.model.progress.ProgressInfo;
import org.full.migration.model.progress.ProgressStatus;
import org.full.migration.model.table.Table;
import org.full.migration.model.table.TableAutoIncrement;
import org.full.migration.model.table.TableData;
import org.full.migration.model.table.TableForeignKey;
import org.full.migration.model.table.TableIndex;
import org.full.migration.model.table.TableMeta;
import org.full.migration.model.table.TablePrimaryKey;
import org.full.migration.target.index.OgracIndexBuilder;
import org.full.migration.utils.DatabaseUtils;
import org.full.migration.utils.MigrationErrorLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

/**
 * OgracTargetDatabase
 * DataX-based target database implementation for handling migration scenarios
 * that require DataX, such as Oracle to Ograc
 *
 * @since 2026-03-09
 */
public class OgracTargetDatabase extends AbstractTargetDatabase {
    private static final Logger LOGGER = LoggerFactory.getLogger(OgracTargetDatabase.class);
    private static final String CREATE_FK_SQL = "ALTER TABLE \"%s\".\"%s\" ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES \"%s\".\"%s\" (%s)";

    /**
     * SQL for creating primary key， ograc primary key constraint name will create automatically;
     * constraint name does not support by source table system default name
     */
    private static final String CREATE_PK_SQL = "ALTER TABLE %s.%s ADD PRIMARY KEY (%s)";
    private static final String CREATE_AUTO_INCREMENT_SQL = "ALTER TABLE %s.%s  MODIFY %s AUTO_INCREMENT";
    private static final String RESET_AUTO_INCREMENT_SQL = "ALTER TABLE %s.%s  AUTO_INCREMENT=%d";

    private final SourceConfig sourceConfig;
    private final DataXManager dataXManager;
    private final OgracIndexBuilder indexBuilder;

    /**
     * Constructor
     *
     * @param globalConfig Global configuration object containing source and target
     *                     database configuration information
     * @throws MigrationException If DataX initialization fails
     */
    public OgracTargetDatabase(GlobalConfig globalConfig) throws MigrationException {
        super(globalConfig);
        this.sourceConfig = globalConfig.getSourceConfig();
        this.dataXManager = new DataXManager(globalConfig);
        this.indexBuilder = new OgracIndexBuilder();
        // For DataX scenarios, use OgracConnection
        super.connection = new OgracConnection();
    }

    /**
     * Write table data
     * For DataX scenarios, directly process table metadata from
     * TargetTableMetaQueue, use DataX to read data from source database and write
     * to target database
     */
    @Override
    public void writeTable() {
        try {
            while (!QueueManager.getInstance().isQueuePollEnd(QueueManager.TARGET_TABLE_META_QUEUE)) {
                TableMeta tableMeta = (TableMeta) QueueManager.getInstance()
                        .pollQueue(QueueManager.TARGET_TABLE_META_QUEUE);
                if (tableMeta == null) {
                    LOGGER.debug("TableMeta is null, continuing to next iteration...");
                    continue;
                }
                Table table = tableMeta.getTable();
                LOGGER.info("start to migrate table:{}.", table.getTableName());
                try {
                    TableData tableData = new TableData(table, null, null, null);
                    copyData(tableData);
                } catch (Exception e) {
                    LOGGER.warn("fail to write table {}.{}, errMsg:{}", table.getSchemaName(),
                            table.getTableName(), e.getMessage());
                }
            }
            LOGGER.info("{} finished to write table.", Thread.currentThread().getName());
        } catch (Exception e) {
            LOGGER.warn("fail to write table, errMsg:{}", e.getMessage());
        }
        Thread.currentThread().interrupt();
    }

    /**
     * Write database objects (such as stored procedures, functions, etc.)
     * Get objects from OBJECT_QUEUE, execute creation SQL statements, and try to
     * convert using SQL translator if failed
     *
     * @param sourceDbType Source database type
     * @param objectType   Object type
     */
    @Override
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
                    if (objectType.equalsIgnoreCase("SEQUENCE")) {
                        executeDropSequence(targetSchema, conn, statement, object.getName());
                    }
                    executeCreateObject(targetSchema, conn, statement, createObjectSql);
                    LOGGER.info("Successfully created {} {}", objectType, sourceFullName.getFullName());
                } catch (SQLException e) {
                    LOGGER.error("fail to write [{}][ {} ] with sql: [{}], error: ", objectType,
                            sourceFullName.getFullName(), createObjectSql, e);
                    MigrationErrorLogger.getInstance().logSqlError(
                            "CREATE " + objectType,sourceFullName.getFullName(),
                            createObjectSql, e.getMessage());
                }
            }
            LOGGER.info("{} has finished to write {}.", Thread.currentThread().getName(), objectType);
        } catch (SQLException e) {
            LOGGER.warn("write {} has occurred an exception, detail:{}", objectType, e.getMessage());
        }
    }

    private void executeDropSequence(String targetSchema, Connection conn, Statement statement, String fullName)
            throws SQLException {
        try {
            conn.setAutoCommit(false);
            conn.setSchema(targetSchema);
            statement.execute("DROP SEQUENCE IF EXISTS " + fullName);
            conn.commit();
        } catch (SQLException e) {
            conn.rollback();
            throw e;
        }
    }

    private void copyData(TableData tableTask) throws Exception {
        String schemaName = tableTask.getTable().getSchemaName();
        String tableName = tableTask.getTable().getTableName();
        String fullName = schemaName + "." + tableName;
        ProgressInfo progressInfo = new ProgressInfo();
        progressInfo.setSchema(schemaName);
        progressInfo.setName(tableName);
        LOGGER.info("Starting data migration for table: {}.{}", schemaName, tableName);
        try {
            DataXConfigContext context = new DataXConfigContext(
                    sourceConfig.getDbConn(),
                    dbConfig,
                    schemaName,
                    tableName,
                    tableTask.getTable().getTargetSchemaName(),
                    tableTask.getTable());
            LOGGER.debug("Created DataXConfigContext for table: {}.{}", schemaName, tableName);
                
            if (dataXManager.executeMigration(context)) {
                progressInfo.setPercent(1);
                progressInfo.setStatus(ProgressStatus.MIGRATED_COMPLETE.getCode());
                LOGGER.info("DataX migration of {}.{} succeeded", schemaName, tableName);
            } else {
                progressInfo.setStatus(ProgressStatus.MIGRATED_FAILURE.getCode());
                progressInfo.setPercent(ProgressStatus.MIGRATED_FAILURE.getCode());
                progressInfo.setError("DataX migration failed");
                LOGGER.error("DataX migration of {}.{} failed", schemaName, tableName);
                MigrationErrorLogger.getInstance().logSqlError(
                        "DataX Migration",
                        schemaName + "." + tableName,
                        "DataX migration execution",
                        "DataX migration failed");
            }
        } catch (Exception e) {
            progressInfo.setStatus(ProgressStatus.MIGRATED_FAILURE.getCode());
            progressInfo.setPercent(ProgressStatus.MIGRATED_FAILURE.getCode());
            progressInfo.setError(e.getMessage());
            LOGGER.error("Exception during DataX migration of {}.{}, error message:{}",
                    schemaName, tableName, e.getMessage(), e);
            MigrationErrorLogger.getInstance().logSqlError(
                    "DataX Migration",
                    schemaName + "." + tableName,
                    "DataX migration execution",
                    e.getMessage());
            throw e;
        } finally {
            if (isJsonDump) {
                ProgressTracker.getInstance().upgradeTableProgress(fullName, progressInfo);
            }
            LOGGER.debug("Updated progress for table: {}.{}", schemaName, tableName);
        }
    }

    /**
     * For DataX scenarios, check users exist and it must be has necessary permissions
     * if not ,then throw MigrationException
     * @param schemas Set of schemas to create
     * @throws MigrationException If user does not exist or lacks necessary permissions
     */
    @Override
    public void createSchemas(Set<String> schemas) throws MigrationException {
        if (CollectionUtils.isEmpty(schemas)) {
            LOGGER.info("No schemas to process");
            return;
        }
        try (Connection conn = connection.getConnection(dbConfig)) {
            for (String schema : schemas) {
                if (StringUtils.isBlank(schema)) {
                    continue;
                }
                try {
                    boolean userExists = checkUserExists(conn, schema);
                    if (!userExists) {
                        String errorMsg = "User " + schema + " does not exist";
                        LOGGER.error(errorMsg);
                        MigrationErrorLogger.getInstance().logSqlError(
                                "CHECK USER EXISTENCE", schema,"", errorMsg);
                        throw new MigrationException(ErrorCode.USER_NOT_EXIST.getCode(), errorMsg);
                    }
                    checkAndGrantPermissions(conn, schema);
                    LOGGER.info("Verified permissions for user: {}", schema);
                } catch (SQLException e) {
                    String errorMsg = "Error checking user " + schema + ": " + e.getMessage();
                    LOGGER.error(errorMsg);
                    MigrationErrorLogger.getInstance().logSqlError( "CHECK USER PERMISSIONS",
                            schema, "", e.getMessage());
                    throw new MigrationException(ErrorCode.USER_NOT_PERMISSION.getCode(), errorMsg, errorMsg, e);
                }
            }
        } catch (SQLException e) {
            String errorMsg = "Error connecting to database: " + e.getMessage();
            LOGGER.error(errorMsg);
            throw new MigrationException(ErrorCode.CONNECTION_FAILED.getCode(), errorMsg, errorMsg, e);
        }
    }

    /**
     * Check if user exists in the database
     *
     * @param conn Database connection
     * @param username Username to check
     * @return True if user exists, false otherwise
     * @throws SQLException If an error occurs
     */
    private boolean checkUserExists(Connection conn, String username) throws SQLException {
        String checkUserSql = "SELECT 1 FROM ADM_USERS WHERE username = ?";
        try (PreparedStatement pstmt = conn.prepareStatement(checkUserSql)) {
            pstmt.setString(1, username.toLowerCase(Locale.ROOT));
            try (ResultSet rs = pstmt.executeQuery()) {
                return rs.next();
            }
        }
    }

    /**
     * Check and grant necessary permissions for the user
     *
     * @param conn Database connection
     * @param username Username to grant permissions to
     * @throws SQLException If an error occurs
     */
    private void checkAndGrantPermissions(Connection conn, String username) throws SQLException {
        String[] requiredPermissions = {"CREATE TABLE","CREATE VIEW","CREATE PROCEDURE","CREATE FUNCTION",
            "CREATE SEQUENCE","CREATE TRIGGER"};

        conn.setAutoCommit(false);
        try (Statement statement = conn.createStatement()) {
            for (String permission : requiredPermissions) {
                String checkPermissionSql = String.format(
                    "SELECT 1 FROM ADM_TAB_PRIVS WHERE grantee = '%s' AND privilege = '%s'",
                    username.toUpperCase(Locale.ROOT),
                    permission
                );
                try (ResultSet rs = statement.executeQuery(checkPermissionSql)) {
                    if (!rs.next()) {
                        // Grant permission if not already granted
                        String grantPermissionSql = String.format(
                            "GRANT %s TO %s",
                            permission,
                            username
                        );
                        statement.execute(grantPermissionSql);
                        LOGGER.info("Granted {} to user: {}", permission, username);
                    }
                }
            }
            conn.commit();
        } catch (SQLException e) {
            conn.rollback();
            throw e;
        }
    }

    /**
     * Write table indexes
     * For DataX scenarios, indexes are created together with the table, no need for
     * separate processing
     */
    @Override
    public void writeTableIndex() {
        writeKeyOrIndex(object -> getCreateIndexSql((TableIndex) object), QueueManager.TABLE_INDEX_QUEUE,
                "table index");
    }

    private Optional<String> getCreateIndexSql(TableIndex tableIndex) {
        return indexBuilder.buildCreateIndexSql(tableIndex);
    }



    /**
     * Write table primary keys
     * For DataX scenarios, primary keys are created together with the table, no
     * need for separate processing
     */
    @Override
    public void writeTablePk() {
        writeKeyOrIndex(object -> getCreatePkSql((TablePrimaryKey) object), QueueManager.TABLE_PRIMARY_KEY_QUEUE,
                "table primary key");
        // Write auto increment columns
        writeAutoIncrement();
    }

    private void writeAutoIncrement() {
        String logPrefix = "table auto increment ";
        try (Connection conn = connection.getConnection(dbConfig); Statement statement = conn.createStatement()) {
            String executeSql = "";
            while (!QueueManager.getInstance().isQueuePollEnd(QueueManager.TABLE_AUTO_INCREMENT_QUEUE)) {
                Object object = QueueManager.getInstance().pollQueue(QueueManager.TABLE_AUTO_INCREMENT_QUEUE);
                if (object == null) {
                    LOGGER.debug("{} poll from queue is null, to write {}.", Thread.currentThread().getName(),
                            logPrefix);
                    continue;
                }
                if (object instanceof TableAutoIncrement tableAutoIncrement) {
                    try {
                        executeSql = String.format(CREATE_AUTO_INCREMENT_SQL, tableAutoIncrement.getSchemaName(),
                                tableAutoIncrement.getTableName(), tableAutoIncrement.getColumnName());
                        statement.executeUpdate(executeSql);
                        LOGGER.info("write {}  [{}] success", logPrefix, executeSql);
                        executeSql = String.format(RESET_AUTO_INCREMENT_SQL, tableAutoIncrement.getSchemaName(),
                                tableAutoIncrement.getTableName(), tableAutoIncrement.getMaxAutoIncrement());
                        statement.executeUpdate(executeSql);
                        LOGGER.info("write {}  [{}] success", logPrefix, executeSql);
                    } catch (SQLException e) {
                        LOGGER.error("write {} has occurred an exception, detail: {} {}", logPrefix, executeSql,
                                e.getMessage());
                        MigrationErrorLogger.getInstance().logSqlError(logPrefix, object.toString(), executeSql,
                                e.getMessage());
                        continue;
                    }
                }
                LOGGER.info("{} has finished to write {}", Thread.currentThread().getName(), logPrefix);
            }
        } catch (SQLException e) {
            LOGGER.warn("Initial connection error while writing {}, detail: {}", logPrefix, e.getMessage());
        }
    }

    private Optional<String> getCreatePkSql(TablePrimaryKey tablePrimaryKey) {
        return Optional.of(String.format(CREATE_PK_SQL, tablePrimaryKey.getSchemaName(), tablePrimaryKey.getTableName(),
                DatabaseUtils.formatMultiColName(tablePrimaryKey.getColumnName())));
    }

    /**
     * Write table foreign keys
     * For DataX scenarios, foreign keys are created together with the table, no
     * need for separate processing
     */
    @Override
    public void writeTableFk() {
        writeKeyOrIndex(object -> getCreateFkSql((TableForeignKey) object), QueueManager.TABLE_FOREIGN_KEY_QUEUE,
                "table foreign key");
    }

    private Optional<String> getCreateFkSql(TableForeignKey tableForeignKey) {
        return Optional.of(
                String.format(CREATE_FK_SQL, tableForeignKey.getSchemaName(), tableForeignKey.getParentTable(),
                        tableForeignKey.getFkName(), tableForeignKey.getParentColumn(), tableForeignKey.getSchemaName(),
                        tableForeignKey.getReferencedTable(),DatabaseUtils.formatMultiColName(tableForeignKey.getReferencedColumn())));
    }

    /**
     * Write table structure
     * For DataX scenarios, table structure is automatically handled through DataX
     * configuration, no need for separate processing
     */
    @Override
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
                    if (isTableExist(conn, table)) {
                        continue;
                    }
                    LOGGER.info("start to migration table:{}", table.getTableName());
                    executeTableConstructor(tableMeta, conn);
                    QueueManager.getInstance().putToQueue(QueueManager.TARGET_TABLE_META_QUEUE, tableMeta);
                } catch (SQLException e) {
                    conn.rollback();
                    LOGGER.error("fail to create table {}.{}, errMsg:{}  \n {} ", table.getTargetSchemaName(),
                            table.getTableName(), e.getMessage(), tableMeta.getCreateTableSql());
                    MigrationErrorLogger.getInstance().logCreateTableError(
                            table.getTargetSchemaName(),
                            table.getTableName(),
                            tableMeta.getCreateTableSql(),
                            e.getMessage());
                }
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

    private boolean isTableExist(Connection conn, Table table) throws SQLException {
        try (PreparedStatement pstmt = conn
                .prepareStatement("SELECT 1 FROM ADM_TABLES WHERE table_name = ? AND OWNER = ?")) {
            pstmt.setString(1, table.getTableName().toLowerCase(Locale.ROOT));
            pstmt.setString(2, table.getTargetSchemaName().toLowerCase(Locale.ROOT));
            try (ResultSet rs = pstmt.executeQuery()) {
                return rs.next();
            }
        }
    }

    /**
     * Execute table constructor SQL
     * @param tableMeta Table metadata object
     * @param conn      Database connection
     * @throws SQLException If an error occurs
     */
    protected void executeTableConstructor(TableMeta tableMeta, Connection conn) throws SQLException {
        try (Statement statement = conn.createStatement()) {
            Table table = tableMeta.getTable();
            conn.setAutoCommit(false);
            conn.setSchema(table.getTargetSchemaName());
            statement.execute(String.format(DROP_TABLE_SQL, table.getTableName()));
            String createTableSql = tableMeta.getCreateTableSql();
            
            executeCreateTableSql(statement, createTableSql, table);
            
            conn.commit();
            createdTables.add(table.getTargetSchemaName() + "." + table.getTableName());
            LOGGER.info("create {}.{} success", table.getTargetSchemaName(), table.getTableName());
        }
    }
    
    private void executeCreateTableSql(Statement statement, String createTableSql, Table table) throws SQLException {
        try {
            if (createTableSql.contains(";")) {
                String[] sqls = createTableSql.split(";");
                for (String sql : sqls) {
                    if (StringUtils.isNotBlank(sql)) {
                        statement.execute(sql);
                    }
                }
            } else {
                statement.execute(createTableSql);
            }
        } catch (SQLException e) {
            if (isTablespaceNotExistError(e)) {
                LOGGER.warn("Tablespace not found in create table SQL, removing TABLESPACE clauses and retrying: {}", 
                        table.getTableName());
                String sqlWithoutTablespace = removeTablespaceClause(createTableSql);
                if (sqlWithoutTablespace.contains(";")) {
                    String[] sqls = sqlWithoutTablespace.split(";");
                    for (String sql : sqls) {
                        if (StringUtils.isNotBlank(sql)) {
                            statement.execute(sql);
                        }
                    }
                } else {
                    statement.execute(sqlWithoutTablespace);
                }
                LOGGER.info("create {}.{} success after removing tablespace", table.getTargetSchemaName(), table.getTableName());
            } else {
                throw e;
            }
        }
    }
    
    private boolean isTablespaceNotExistError(SQLException e) {
        String message = e.getMessage().toLowerCase(Locale.ROOT);
        return message.contains("tablespace") && message.contains("does not exist");
    }
    
    private String removeTablespaceClause(String sql) {
        return sql.replaceAll("\\s+TABLESPACE\\s+[A-Za-z0-9_]+", "");
    }
    
    /**
     * Insert table snapshot information
     * For DataX scenarios, no need to record snapshot information
     *
     * @param conn      Database connection
     * @param tableData Table data object
     */
    @Override
    public void insertTableSnapshotInfo(Connection conn, TableData tableData) {
        LOGGER.debug("DataX scenario: skipping snapshot info insertion for table {}.{}",
                tableData.getTable().getSchemaName(), tableData.getTable().getTableName());
    }

    /**
     * Drop replica schema
     * For DataX scenarios, no need to drop replica schema
     */
    @Override
    public void dropReplicaSchema() {
        LOGGER.debug("DataX scenario: skipping replica schema drop");
    }

    /**
     * Create custom or domain types
     * For DataX scenarios, handle custom type migration
     *
     * @param customTypes List of custom type metadata
     */
    @Override
    public void createCustomOrDomainTypes(List<PostgresCustomTypeMeta> customTypes) {
        LOGGER.info("DataX scenario: processing custom types");
        if (CollectionUtils.isEmpty(customTypes)) {
            LOGGER.info("There are no custom types to be migrated.");
            return;
        }
        LOGGER.info("Finished processing custom types for DataX scenario");
    }

    /**
     * Execute SQL statements for creating objects
     *
     * @param sinkSchema Target schema
     * @param conn       Database connection
     * @param statement  Statement object
     * @param sqlStr     SQL statement
     * @throws SQLException SQL exception
     */
    private void executeCreateObject(String sinkSchema, Connection conn,
            Statement statement, String sqlStr) throws SQLException {
        try {
            conn.setAutoCommit(false);
            conn.setSchema(sinkSchema);
            statement.execute(sqlStr);
            conn.commit();
        } catch (SQLException e) {
            conn.rollback();
            throw e;
        }
    }
    
    /**
     * Shutdown the target database instance and release resources
     */
    @Override
    public void shutdown() {
        if (dataXManager != null) {
            LOGGER.info("Shutting down DataXManager...");
            dataXManager.shutdown();
            LOGGER.info("DataXManager shutdown completed");
        }
    }
}