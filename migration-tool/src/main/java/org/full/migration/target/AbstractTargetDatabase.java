/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

package org.full.migration.target;

import org.apache.commons.lang3.StringUtils;
import org.full.migration.coordinator.ProgressTracker;
import org.full.migration.coordinator.QueueManager;
import org.full.migration.enums.SqlCompatibilityEnum;
import org.full.migration.exception.DatabaseConnectionException;
import org.full.migration.exception.ErrorCode;
import org.full.migration.jdbc.JdbcConnection;
import org.full.migration.model.config.DatabaseConfig;
import org.full.migration.model.config.GlobalConfig;
import org.full.migration.model.progress.ProgressStatus;
import org.full.migration.model.table.Table;
import org.full.migration.model.table.TableForeignKey;
import org.full.migration.model.table.TableIndex;
import org.full.migration.model.table.TableMeta;
import org.full.migration.model.table.TablePrimaryKey;
import org.full.migration.utils.MigrationErrorLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * TargetDatabase
 *
 * @since 2025-04-18
 */
public abstract class AbstractTargetDatabase implements ITargetDatabase {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractTargetDatabase.class);
    
    protected static final String DROP_TABLE_SQL = "drop table if exists \"%s\"";

    protected DatabaseConfig dbConfig;
    protected JdbcConnection connection;
    protected Map<String, String> schemaMappings;
    protected List<String> createdTables = new ArrayList<>();
    protected boolean isJsonDump;
    /**
     * Constructor
     *
     * @param globalConfig globalConfig
     */
    public AbstractTargetDatabase(GlobalConfig globalConfig) {
        this.dbConfig = globalConfig.getOgConn();
        this.isJsonDump = globalConfig.getIsDumpJson();
        this.schemaMappings = globalConfig.getSourceConfig().getSchemaMappings();
    }

    @Override
    public void checkConnection() throws DatabaseConnectionException {
        try (Connection conn = connection.getConnection(dbConfig)) {
            conn.isValid(10);
        } catch (SQLException e) {
            LOGGER.error("Error validating connection: {}", e.getMessage());
            throw new DatabaseConnectionException(ErrorCode.CONNECTION_FAILED.getCode(),"Connection validation failed", e);
        }
    }

    protected void copyMeta(TableMeta tableMeta, Connection conn) throws SQLException {
        try (Statement statement = conn.createStatement()) {
            Table table = tableMeta.getTable();
            conn.setAutoCommit(false);
            conn.setSchema(table.getTargetSchemaName());
            statement.execute(String.format(DROP_TABLE_SQL,
                    table.getTableName().replace("\"", "\"\"")));
            statement.execute(tableMeta.getCreateTableSql());
            conn.commit();
            createdTables.add(table.getTargetSchemaName() + "." + table.getTableName());
            LOGGER.info("create {}.{} success", table.getTargetSchemaName(), table.getTableName());
        }
    }

     /**
     * writeKeyOrIndex
     *
     * @param sqlGenerator sqlGenerator
     * @param queueName queueName
     * @param logPrefix logPrefix
     */
    public void writeKeyOrIndex(Function<Object, Optional<String>> sqlGenerator, String queueName, String logPrefix) {
        Connection conn = null;
        Statement statement = null;
        try {
            conn = connection.getConnection(dbConfig);
            statement = conn.createStatement();
            while (!QueueManager.getInstance().isQueuePollEnd(queueName)) {
                Object object = QueueManager.getInstance().pollQueue(queueName);
                if (object == null) {
                    LOGGER.debug("{} poll from queue is null, to write {}.", Thread.currentThread().getName(),
                        logPrefix);
                    continue;
                }
                String sql = "";
                boolean success = false;
                boolean reconnected = false;
                boolean rejected = false;
                while (!success) {
                    try {
                        sql = sqlGenerator.apply(object)
                                .orElseThrow(() -> new SQLException("This object " + object + " is not currently supported."));
                        if (!isSafeDdl(sql)) {
                            LOGGER.error("write {} has been rejected because the SQL contains unsafe content: [{}]",
                                logPrefix, sql);
                            MigrationErrorLogger.getInstance().logSqlError(logPrefix, object.toString(), sql,
                                "rejected: unsafe ddl");
                            rejected = true;
                            break;
                        }
                        statement.executeUpdate(sql);
                        LOGGER.info("write {}  [{}] success", logPrefix, sql);
                        success = true;
                    } catch (SQLException e) {
                        LOGGER.error("write {} has occurred an exception,  detail: {} {}", logPrefix, sql, e.getMessage());
                        MigrationErrorLogger.getInstance().logSqlError(logPrefix, object.toString(), sql, e.getMessage());
                        if (reconnected || !isConnectionBroken(conn)) {
                            break;
                        }
                        LOGGER.warn("target connection is broken, reconnecting and retrying: {}", sql);
                        closeQuietly(statement);
                        closeQuietly(conn);
                        try {
                            conn = connection.getConnection(dbConfig);
                            statement = conn.createStatement();
                            reconnected = true;
                        } catch (SQLException reconnectEx) {
                            LOGGER.error("reconnect to target database failed, detail:{}", reconnectEx.getMessage());
                            break;
                        }
                    }
                }
                if (rejected) {
                    continue;
                }
                if (!success) {
                    continue;
                }
                LOGGER.info("{} has finished to write {}", Thread.currentThread().getName(), logPrefix);

                if (isJsonDump) {
                    if (object instanceof TablePrimaryKey) {
                        TablePrimaryKey tablePrimaryKey = (TablePrimaryKey) object;
                        ProgressTracker.getInstance()
                            .upgradeKeyAndIndexProgressMap(tablePrimaryKey.getSchemaName()+tablePrimaryKey.getPkName(), ProgressStatus.MIGRATED_COMPLETE, StringUtils.EMPTY);
                    } else if (object instanceof TableForeignKey) {
                        TableForeignKey  tableForeignKey= (TableForeignKey) object;
                        ProgressTracker.getInstance()
                                .upgradeKeyAndIndexProgressMap(tableForeignKey.getSchemaName()+tableForeignKey.getFkName(), ProgressStatus.MIGRATED_COMPLETE, StringUtils.EMPTY);
                    } else if (object instanceof TableIndex) {
                        TableIndex  tableIndex= (TableIndex) object;
                        ProgressTracker.getInstance()
                                .upgradeKeyAndIndexProgressMap(tableIndex.getSchemaName()+tableIndex.getIndexName(), ProgressStatus.MIGRATED_COMPLETE, StringUtils.EMPTY);
                    }
                }
            }
        } catch (SQLException e) {
            LOGGER.warn("Initial connection error while writing {}, detail: {}", logPrefix, e.getMessage());
        } finally {
            closeQuietly(statement);
            closeQuietly(conn);
        }
    }

    @Override
    public void writeConstraints() {
        Connection conn = null;
        Statement statement = null;
        try {
            conn = connection.getConnection(dbConfig);
            statement = conn.createStatement();
            while (!QueueManager.getInstance().isQueuePollEnd(QueueManager.TABLE_CONSTRAINT_QUEUE)) {
                String alterSql = (String) QueueManager.getInstance().pollQueue(QueueManager.TABLE_CONSTRAINT_QUEUE);
                if (alterSql == null) {
                    LOGGER.debug("{} poll from queue is null, to write table constraints.",
                        Thread.currentThread().getName());
                    continue;
                }
                if (!isSafeDdl(alterSql)) {
                    LOGGER.error("write table constraints has been rejected because the SQL contains unsafe content: [{}]",
                        alterSql);
                    MigrationErrorLogger.getInstance().logSqlError("writeConstraints", "", alterSql,
                        "rejected: unsafe ddl");
                    continue;
                }
                boolean success = false;
                boolean reconnected = false;
                while (!success) {
                    try {
                        conn.setAutoCommit(false);
                        statement.execute(alterSql);
                        conn.commit();
                        success = true;
                    } catch (SQLException e) {
                        try {
                            conn.rollback();
                        } catch (SQLException rollbackEx) {
                            LOGGER.debug("rollback table constraints failed, connection may be broken: {}",
                                rollbackEx.getMessage());
                        }
                        if (e.getMessage() != null && !e.getMessage().endsWith("already exists")) {
                            LOGGER.error("write table constraints has occurred an exception,  detail:{}", e.getMessage());
                        }
                        MigrationErrorLogger.getInstance().logSqlError("writeConstraints", "", alterSql, e.getMessage());
                        if (reconnected || !isConnectionBroken(conn)) {
                            break;
                        }
                        LOGGER.warn("target connection is broken, reconnecting and retrying: {}", alterSql);
                        closeQuietly(statement);
                        closeQuietly(conn);
                        try {
                            conn = connection.getConnection(dbConfig);
                            statement = conn.createStatement();
                            reconnected = true;
                        } catch (SQLException reconnectEx) {
                            LOGGER.error("reconnect to target database failed, detail:{}", reconnectEx.getMessage());
                            break;
                        }
                    }
                }
                if (!success) {
                    continue;
                }
                LOGGER.info("{} has finished to write table constraints", Thread.currentThread().getName());
            }
        } catch (SQLException e) {
            LOGGER.warn("Initial connection error while writing table constraints, detail: {}", e.getMessage());
        } finally {
            closeQuietly(statement);
            closeQuietly(conn);
        }
    }
    
    /**
     * isConnectionBroken
     * Check whether the connection is still usable. When the target server terminates the backend
     * (e.g. "FATAL: terminating connection due to administrator command"), the existing connection
     * is closed and every subsequent write fails, so a new connection must be established.
     *
     * @param conn connection
     * @return true if the connection is broken
     */
    protected boolean isConnectionBroken(Connection conn) {
        try {
            return conn == null || conn.isClosed() || !conn.isValid(2);
        } catch (SQLException e) {
            return true;
        }
    }

    /**
     * closeQuietly
     * Close the resource without throwing any exception.
     *
     * @param closeable closeable
     */
    protected void closeQuietly(AutoCloseable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (Exception e) {
                LOGGER.debug("close resource failed: {}", e.getMessage());
            }
        }
    }

    /**
     * Check whether a DDL statement is safe to execute. A statement is considered unsafe
     * when it contains a statement separator ({@code ;}) or comment markers ({@code --},
     * {@code /*}, {@code *}{@code /}) outside of quoted identifiers and string literals,
     * because those characters could be used to smuggle additional SQL statements past
     * the single-Statement execution boundary.
     *
     * @param sql the generated DDL to validate
     * @return true when the statement is safe to execute
     */
    protected boolean isSafeDdl(String sql) {
        if (sql == null) {
            return false;
        }
        boolean inSingleQuote = false;
        boolean inDoubleQuote = false;
        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            char next = i + 1 < sql.length() ? sql.charAt(i + 1) : '\0';
            if (inSingleQuote) {
                if (c == '\'') {
                    if (next == '\'') {
                        i++;
                    } else {
                        inSingleQuote = false;
                    }
                }
                continue;
            }
            if (inDoubleQuote) {
                if (c == '"') {
                    if (next == '"') {
                        i++;
                    } else {
                        inDoubleQuote = false;
                    }
                }
                continue;
            }
            if (c == '\'') {
                inSingleQuote = true;
            } else if (c == '"') {
                inDoubleQuote = true;
            } else if (c == ';') {
                return false;
            } else if (c == '-' && next == '-') {
                return false;
            } else if (c == '/' && next == '*') {
                return false;
            } else if (c == '*' && next == '/') {
                return false;
            }
        }
        return true;
    }

    /**
     * Split a SQL script into individual statements, honoring quoted identifiers,
     * string literals and comments. A bare {@code ;} inside a string literal or a quoted
     * identifier (for example an Oracle DDL with a literal containing an escaped quote)
     * must not terminate the current statement, otherwise an attacker-controlled value
     * could be turned into a standalone injected statement.
     *
     * @param sql the SQL script to split
     * @return list of non-blank statements
     */
    protected List<String> splitSqlStatements(String sql) {
        List<String> statements = new ArrayList<>();
        if (sql == null || sql.isEmpty()) {
            return statements;
        }
        StringBuilder current = new StringBuilder();
        boolean inSingleQuote = false;
        boolean inDoubleQuote = false;
        boolean inLineComment = false;
        boolean inBlockComment = false;
        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            char next = i + 1 < sql.length() ? sql.charAt(i + 1) : '\0';
            if (inLineComment) {
                current.append(c);
                if (c == '\n') {
                    inLineComment = false;
                }
                continue;
            }
            if (inBlockComment) {
                current.append(c);
                if (c == '*' && next == '/') {
                    current.append('/');
                    i++;
                    inBlockComment = false;
                }
                continue;
            }
            if (inSingleQuote) {
                current.append(c);
                if (c == '\'') {
                    if (next == '\'') {
                        current.append('\'');
                        i++;
                    } else {
                        inSingleQuote = false;
                    }
                }
                continue;
            }
            if (inDoubleQuote) {
                current.append(c);
                if (c == '"') {
                    if (next == '"') {
                        current.append('"');
                        i++;
                    } else {
                        inDoubleQuote = false;
                    }
                }
                continue;
            }
            if (c == '-' && next == '-') {
                inLineComment = true;
                current.append(c).append(next);
                i++;
            } else if (c == '/' && next == '*') {
                inBlockComment = true;
                current.append(c).append(next);
                i++;
            } else if (c == '\'') {
                inSingleQuote = true;
                current.append(c);
            } else if (c == '"') {
                inDoubleQuote = true;
                current.append(c);
            } else if (c == ';') {
                String statement = current.toString().trim();
                if (!statement.isEmpty()) {
                    statements.add(statement);
                }
                current.setLength(0);
            } else {
                current.append(c);
            }
        }
        String last = current.toString().trim();
        if (!last.isEmpty()) {
            statements.add(last);
        }
        return statements;
    }
    
    /**
     * Shutdown the target database instance and release resources
     * Default implementation does nothing
     * Subclasses can override this method to release specific resources
     */
    @Override
    public void shutdown() {
    }
}
