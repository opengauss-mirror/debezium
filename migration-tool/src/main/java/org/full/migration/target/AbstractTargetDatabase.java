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
            statement.execute(String.format(DROP_TABLE_SQL, table.getTableName()));
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
        try (Connection conn = connection.getConnection(dbConfig); Statement statement = conn.createStatement()) {
            while (!QueueManager.getInstance().isQueuePollEnd(queueName)) {
                Object object = QueueManager.getInstance().pollQueue(queueName);
                if (object == null) {
                    LOGGER.debug("{} poll from queue is null, to write {}.", Thread.currentThread().getName(),
                        logPrefix);
                    continue;
                }
                String sql="";
                try {
                    sql = sqlGenerator.apply(object)
                            .orElseThrow(() -> new SQLException("This object " + object + " is not currently supported."));
                    statement.executeUpdate(sql);
                    LOGGER.info("write {}  [{}] success", logPrefix, sql);
                } catch (SQLException e) {
                    LOGGER.error("write {} has occurred an exception,  detail: {}", logPrefix, e.getMessage());
                    MigrationErrorLogger.getInstance().logSqlError(logPrefix, object.toString(), sql, e.getMessage() );
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
        }
    }

    @Override
    public void writeConstraints() {
         try (Connection conn = connection.getConnection(dbConfig); Statement statement = conn.createStatement()) {
            while (!QueueManager.getInstance().isQueuePollEnd(QueueManager.TABLE_CONSTRAINT_QUEUE)) {
                String alterSql = (String) QueueManager.getInstance().pollQueue(QueueManager.TABLE_CONSTRAINT_QUEUE);
                if (alterSql == null) {
                    LOGGER.debug("{} poll from queue is null, to write table constraints.",
                        Thread.currentThread().getName());
                    continue;
                }
                try {
                    conn.setAutoCommit(false);
                    statement.execute(alterSql);
                    conn.commit();
                } catch (SQLException e) {
                    conn.rollback();
                    if (e.getMessage() != null && !e.getMessage().endsWith("already exists")) {
                        LOGGER.error("write table constraints has occurred an exception,  detail:{}", e.getMessage());
                    }
                    MigrationErrorLogger.getInstance().logSqlError("writeConstraints","", alterSql, e.getMessage() );
                    continue;
                }
                LOGGER.info("{} has finished to write table constraints", Thread.currentThread().getName());
            }
        } catch (SQLException e) {
            LOGGER.warn("Initial connection error while writing table constraints, detail: {}", e.getMessage());
        }
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
