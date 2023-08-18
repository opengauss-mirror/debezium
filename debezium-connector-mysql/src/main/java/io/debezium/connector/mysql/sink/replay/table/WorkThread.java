/**
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.sink.replay.table;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.PriorityBlockingQueue;

import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mysql.cj.jdbc.exceptions.CommunicationsException;

import io.debezium.connector.breakpoint.BreakPointObject;
import io.debezium.connector.breakpoint.BreakPointRecord;
import io.debezium.connector.mysql.sink.object.ConnectionInfo;
import io.debezium.connector.mysql.sink.object.DmlOperation;
import io.debezium.connector.mysql.sink.object.SinkRecordObject;
import io.debezium.connector.mysql.sink.object.SourceField;
import io.debezium.connector.mysql.sink.object.TableMetaData;
import io.debezium.connector.mysql.sink.util.SqlTools;

/**
 * Description: WorkThread class
 *
 * @author douxin
 * @since 2023-06-26
 */
public class WorkThread extends Thread {
    private static final Logger LOGGER = LoggerFactory.getLogger(WorkThread.class);
    private static final String INSERT = "c";
    private static final String UPDATE = "u";
    private static final String DELETE = "d";

    private SqlTools sqlTools;
    private int successCount;
    private int failCount;
    private ConnectionInfo connectionInfo;
    private BlockingQueue<SinkRecordObject> sinkRecordQueue = new LinkedBlockingDeque<>();
    private Map<String, TableMetaData> oldTableMap = new HashMap<>();
    private Map<String, String> schemaMappingMap;
    private Connection connection;
    private Statement statement;
    private BreakPointRecord breakPointRecord;
    private PriorityBlockingQueue<Long> replayedOffsets;
    private List<String> failSqlList = new ArrayList<>();
    private SinkRecordObject threadSinkRecordObject = null;
    private boolean isTransaction;
    private boolean isConnection = true;
    private boolean isStop = false;

    /**
     * Constructor
     *
     * @param schemaMappingMap Map<String, String> the schema mapping map
     * @param connectionInfo ConnectionInfo the connection information
     * @param sqlTools SqlTools the sql tools
     * @param index int the index
     * @param breakPointRecord the breakpoint
     */
    public WorkThread(Map<String, String> schemaMappingMap, ConnectionInfo connectionInfo,
                      SqlTools sqlTools, int index, BreakPointRecord breakPointRecord) {
        super("work-thread-" + index);
        this.schemaMappingMap = schemaMappingMap;
        this.connectionInfo = connectionInfo;
        this.sqlTools = sqlTools;
        this.breakPointRecord = breakPointRecord;
        this.replayedOffsets = breakPointRecord.getReplayedOffset();
        this.isTransaction = false;
    }

    /**
     * Get thread sink record object
     *
     * @return SinkRecordObject get sink record object
     */
    public SinkRecordObject getThreadSinkRecordObject() {
        return threadSinkRecordObject;
    }

    @Override
    public void run() {
        SinkRecordObject sinkRecordObject = null;
        connection = connectionInfo.createOpenGaussConnection();
        try {
            statement = connection.createStatement();
        }
        catch (SQLException exp) {
            LOGGER.error("SQL exception occurred in work thread", exp);
        }
        String sql = null;
        while (!isStop) {
            try {
                sinkRecordObject = sinkRecordQueue.take();
                sql = constructSql(sinkRecordObject);
                if (!isConnection) {
                    break;
                }
                if ("".equals(sql)) {
                    replayedOffsets.offer(sinkRecordObject.getKafkaOffset());
                    continue;
                }
                statement.executeUpdate(sql);
                successCount++;
                replayedOffsets.offer(sinkRecordObject.getKafkaOffset());
                savedBreakPointInfo(sinkRecordObject);
                if (successCount % 1000 == 0) {
                    threadSinkRecordObject = sinkRecordObject;
                }
            }
            catch (CommunicationsException exp) {
                updateConnectionAndExecuteSql(sql, sinkRecordObject);
            }
            catch (SQLException exp) {
                if (!connectionInfo.checkConnectionStatus(connection)) {
                    return;
                }
                failCount++;
                failSqlList.add(sql);
                LOGGER.error("SQL exception occurred in work thread", exp);
            }
            catch (InterruptedException exp) {
                LOGGER.warn("Interrupted exception occurred", exp);
            }
            catch (DataException exp) {
                failCount++;
                if (sinkRecordObject != null) {
                    oldTableMap.remove(schemaMappingMap.get(sinkRecordObject.getSourceField()
                            .getDatabase()) + "." + sinkRecordObject.getSourceField().getTable());
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
        DmlOperation dmlOperation;
        if (sinkRecordObject.getDataOperation() instanceof DmlOperation) {
            dmlOperation = (DmlOperation) sinkRecordObject.getDataOperation();
        }
        else {
            return "";
        }
        String schemaName = schemaMappingMap.get(sourceField.getDatabase());
        String tableFullName = schemaName + "." + sourceField.getTable();
        TableMetaData tableMetaData;
        if (oldTableMap.containsKey(tableFullName)) {
            tableMetaData = oldTableMap.get(tableFullName);
        }
        else {
            tableMetaData = sqlTools.getTableMetaData(schemaName, sourceField.getTable());
            oldTableMap.put(tableFullName, tableMetaData);
        }
        String sql = "";
        if (tableMetaData == null && !sqlTools.getIsConnection()) {
            isConnection = false;
            LOGGER.error("There is a connection problem with the mysql,"
                    + " check the database status or connection");
            return sql;
        }
        String operation = dmlOperation.getOperation();
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
     * Gets connection.
     *
     * @return the value of connection
     */
    public Connection getConnection() {
        return connection;
    }

    /**
     * Sets the connection.
     *
     * @param connection the connection
     */
    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    /**
     * clear fail sql list
     */
    public void clearFailSqlList() {
        failSqlList.clear();
    }

    private void updateConnectionAndExecuteSql(String sql, SinkRecordObject sinkRecordObject) {
        try {
            statement.close();
            connection.close();
            connection = connectionInfo.createOpenGaussConnection();
            statement = connection.createStatement();
            statement.executeUpdate(sql);
            savedBreakPointInfo(sinkRecordObject);
            successCount++;
        }
        catch (SQLException exp) {
            if (!connectionInfo.checkConnectionStatus(connection)) {
                return;
            }
            LOGGER.error("SQL exception occurred in work thread", exp);
        }
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
     * get fail count
     *
     * @return int the fail count
     */
    public int getFailCount() {
        return failCount;
    }

    /**
     * Sets the isStop.
     *
     * @param isStop boolean isStop
     */
    public void setIsStop(boolean isStop) {
        this.isStop = isStop;
    }

    private void savedBreakPointInfo(SinkRecordObject sinkRecordObject) {
        BreakPointObject tableBpObject = new BreakPointObject();
        SourceField sourceField = sinkRecordObject.getSourceField();
        Long kafkaOffset = sinkRecordObject.getKafkaOffset();
        tableBpObject.setBeginOffset(kafkaOffset);
        tableBpObject.setTimeStamp(LocalDateTime.now().toString());
        if (!sourceField.getGtid().isEmpty()) {
            tableBpObject.setGtid(sinkRecordObject.getSourceField().getGtid());
        }
        breakPointRecord.storeRecord(tableBpObject, isTransaction);
    }
}
