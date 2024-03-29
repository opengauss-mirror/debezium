/**
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.opengauss.sink.replay;

import com.mysql.cj.jdbc.exceptions.CommunicationsException;
import io.debezium.connector.opengauss.sink.object.ConnectionInfo;
import io.debezium.connector.opengauss.sink.object.SinkRecordObject;
import io.debezium.connector.opengauss.sink.object.SourceField;
import io.debezium.connector.opengauss.sink.object.TableMetaData;
import io.debezium.connector.opengauss.sink.object.DmlOperation;
import io.debezium.connector.opengauss.sink.utils.SqlTools;
import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Description: WorkThread class
 *
 * @author wangzhengyuan
 * @since 2022-11-29
 */
public class WorkThread extends Thread {
    private static final Logger LOGGER = LoggerFactory.getLogger(WorkThread.class);
    private static final String INSERT = "c";
    private static final String UPDATE = "u";
    private static final String DELETE = "d";

    private final DateTimeFormatter sqlPattern = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH:mm:ss.SSS");
    private final SqlTools sqlTools;
    private final ConnectionInfo connectionInfo;
    private final BlockingQueue<SinkRecordObject> sinkRecordQueue = new LinkedBlockingDeque<>();
    private final Map<String, TableMetaData> oldTableMap = new HashMap<>();
    private final Map<String, String> schemaMappingMap;
    private final List<String> failSqlList = new ArrayList<>();
    private int successCount;
    private int failCount;
    private Connection connection;
    private Statement statement;
    private SinkRecordObject threadSinkRecordObject = null;
    private boolean isFreeBlock = true;

    /**
     * Constructor
     *
     * @param schemaMappingMap Map<String, String> the schema mapping map
     * @param connectionInfo ConnectionInfo the connection information
     * @param sqlTools SqlTools the sql tools
     * @param index int the index
     */
    public WorkThread(Map<String, String> schemaMappingMap, ConnectionInfo connectionInfo,
                        SqlTools sqlTools, int index) {
        super("work-thread-" + index);
        this.schemaMappingMap = schemaMappingMap;
        this.connectionInfo = connectionInfo;
        this.sqlTools = sqlTools;
    }

    @Override
    public void run() {
        SinkRecordObject sinkRecordObject = null;
        connection = connectionInfo.createMysqlConnection();
        try {
            statement = connection.createStatement();
        } catch (SQLException exp) {
            LOGGER.error("SQL exception occurred in work thread", exp);
        }
        String sql = null;
            while (true) {
                try {
                    sinkRecordObject = sinkRecordQueue.take();
                    sql = constructSql(sinkRecordObject);
                    if ("".equals(sql)) {
                        continue;
                    }
                    statement.executeUpdate(sql);
                    successCount++;
                    threadSinkRecordObject = sinkRecordObject;
                } catch (CommunicationsException exp) {
                    updateConnectionAndExecuteSql(sql);
                } catch (SQLException exp) {
                    failCount++;
                    failSqlList.add("-- " + sqlPattern.format(LocalDateTime.now()) + ": "
                            + sinkRecordObject.getSourceField() + System.lineSeparator()
                            + "-- " + exp.getMessage() + System.lineSeparator() + sql + System.lineSeparator());
                    LOGGER.error("SQL exception occurred in struct date {}", sinkRecordObject.getSourceField());
                    LOGGER.error("The error SQL statement executed is: {}", sql);
                    LOGGER.error("the cause of the exception is {}", exp.getMessage());
                } catch (InterruptedException exp) {
                    LOGGER.warn("Interrupted exception occurred", exp);
                } catch (DataException exp) {
                    failCount++;
                    if (sinkRecordObject != null) {
                        oldTableMap.remove(schemaMappingMap.get(sinkRecordObject.getSourceField()
                                .getSchema()) + "." + sinkRecordObject.getSourceField().getTable());
                    }
                    LOGGER.error("DataException occurred because of invalid field, possible reason is tables "
                            + "of OpenGauss and MySQL have same table name but different table structure.", exp);
                }
            }
    }

    /**
     * Adds data
     *
     * @param sinkRecordObject SinkRecordObject the sinkRecordObject
     */
    public void addData(SinkRecordObject sinkRecordObject) {
        sinkRecordQueue.add(sinkRecordObject);
    }

    /**
     * Constructs sql
     *
     * @param sinkRecordObject SinkRecordObject the sinkRecordObject
     * @return String the sql
     */
    public String constructSql(SinkRecordObject sinkRecordObject) {
        SourceField sourceField = sinkRecordObject.getSourceField();
        DmlOperation dmlOperation = sinkRecordObject.getDmlOperation();
        String operation = dmlOperation.getOperation();
        String schemaName = schemaMappingMap.get(sourceField.getSchema());
        String tableFullName = schemaName + "." + sourceField.getTable();
        TableMetaData tableMetaData;
        if (oldTableMap.containsKey(tableFullName)) {
            tableMetaData = oldTableMap.get(tableFullName);
        } else {
            tableMetaData = sqlTools.getTableMetaData(schemaName, sourceField.getTable());
            oldTableMap.put(tableFullName, tableMetaData);
        }
        String sql = "";
        switch (operation) {
            case INSERT:
                sql = sqlTools.getInsertSql(tableMetaData, dmlOperation.getAfter());
                break;
            case UPDATE:
                sql = sqlTools.getUpdateSql(tableMetaData, dmlOperation.getBefore(), dmlOperation.getAfter());
                break;
            case DELETE:
                sql = sqlTools.getDeleteSql(tableMetaData, dmlOperation.getBefore());
                break;
            default:
                break;
        }
        return sql;
    }

    /**
     * get fail sql list
     *
     * @return List the fail sql list
     */
    public List<String> getFailSqlList() {
        return failSqlList;
    }

    /**
     * get success count
     *
     * @return count of replayed successfully
     */
    public int getSuccessCount() {
        return this.successCount;
    }

    /**
     * clear fail sql list
     */
    public void clearFailSqlList() {
        failSqlList.clear();
    }

    private void updateConnectionAndExecuteSql(String sql) {
        try {
            statement.close();
            connection.close();
            connection = connectionInfo.createMysqlConnection();
            statement = connection.createStatement();
            statement.executeUpdate(sql);
            successCount++;
        } catch (SQLException exp) {
            LOGGER.error("SQL exception occurred in work thread", exp);
        }
    }

    /**
     * get fail count
     *
     * @return int the fail count
     */
    public int getFailCount() {
        return failCount;
    }

    /**
     * Get thread sink record object
     *
     * @return SinkRecordObject get sink record object
     */
    public SinkRecordObject getThreadSinkRecordObject() {
        return threadSinkRecordObject;
    }

    /**
     * Get work thread queue length
     *
     * @return int the queue length in work thread
     */
    public int getQueueLength() {
        return sinkRecordQueue.size();
    }

    /**
     * Gets isFreeBlock
     *
     * @return boolean the isFreeBlock
     */
    public boolean isFreeBlock() {
        return isFreeBlock;
    }

    /**
     * Sets isFreeBlock
     *
     * @param isFreeBlock boolean the isFreeBlock
     */
    public void setFreeBlock(boolean isFreeBlock) {
        this.isFreeBlock = isFreeBlock;
    }
}

