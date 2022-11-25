/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.sink.replay;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Scanner;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;

import io.debezium.connector.mysql.sink.object.TransactionRecordField;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mysql.cj.util.StringUtils;

import io.debezium.connector.mysql.sink.object.ConnectionInfo;
import io.debezium.connector.mysql.sink.object.DataOperation;
import io.debezium.connector.mysql.sink.object.DdlOperation;
import io.debezium.connector.mysql.sink.object.DmlOperation;
import io.debezium.connector.mysql.sink.object.SinkRecordObject;
import io.debezium.connector.mysql.sink.object.SourceField;
import io.debezium.connector.mysql.sink.object.TableMetaData;
import io.debezium.connector.mysql.sink.object.Transaction;
import io.debezium.connector.mysql.sink.task.MySqlSinkConnectorConfig;
import io.debezium.connector.mysql.sink.util.SqlTools;
import io.debezium.data.Envelope;

/**
 * Description: JdbcDbWriter
 * @author douxin
 * @date 2022/10/31
 **/
public class JdbcDbWriter {
    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcDbWriter.class);
    private ConnectionInfo openGaussConnection;
    private SqlTools sqlTools;
    private ArrayList<String> sqlList = new ArrayList<>();
    private Transaction transaction = new Transaction();
    private HashMap<String, TableMetaData> tableMetaDataMap = new HashMap<String, TableMetaData>();
    private BlockingQueue<SinkRecordObject> sinkRecordQueue = new LinkedBlockingDeque<>();
    private BlockingQueue<Transaction> transactionQueue = new LinkedBlockingQueue<>();
    private HashMap<String, Long> dmlEventCountMap = new HashMap<String, Long>();
    private TransactionDispatcher transactionDispatcher;
    private ArrayList<String> changedTableNameList = new ArrayList<>();
    private BlockingQueue<String> feedBackQueue = new LinkedBlockingQueue<>();
    private ArrayList<SinkRecordObject> sinkRecordsArrayList = new ArrayList<>();
    private boolean needStartReplayFlag = false;
    private HashMap<String, String> tableSnapshotHashmap = new HashMap<>();

    /**
     * Constructor
     *
     * @param MySqlSinkConnectorConfig mysql sink connector config
     */
    public JdbcDbWriter(MySqlSinkConnectorConfig config) {
        openGaussConnection = new ConnectionInfo(config.openGaussUrl, config.openGaussUsername, config.openGaussPassword);
        sqlTools = new SqlTools(openGaussConnection.createOpenGaussConnection());
        transactionDispatcher = new TransactionDispatcher(openGaussConnection, transactionQueue, changedTableNameList, feedBackQueue);
    }

    /**
     * Batch write
     *
     * @param Collection<SinkRecord> sink records
     */
    public void batchWrite(Collection<SinkRecord> records) {
        boolean skipFlag = false;
        for (SinkRecord sinkRecord : records) {
            Struct value = (Struct) sinkRecord.value();
            if (value == null) {
                continue;
            }
            try {
                String status = value.getString(TransactionRecordField.STATUS);
                String id = value.getString(TransactionRecordField.ID);
                if (status.toUpperCase().equals(TransactionRecordField.BEGIN)) {
                    sinkRecordsArrayList.clear();
                }
                else {
                    long eventCount = value.getInt64(TransactionRecordField.EVENT_COUNT);
                    if (!skipFlag) {
                        dmlEventCountMap.put(id, eventCount);
                        for (SinkRecordObject sinkRecordObject : sinkRecordsArrayList) {
                            sinkRecordQueue.add(sinkRecordObject);
                        }
                    }
                    else {
                        skipFlag = false;
                    }
                }
            }
            catch (DataException exp) {
                SinkRecordObject sinkRecordObject = new SinkRecordObject();
                SourceField sourceField = new SourceField(value);
                sinkRecordObject.setSourceField(sourceField);
                DataOperation dataOperation = null;
                if (isSkippedEvent(sourceField)) {
                    skipFlag = true;
                    continue;
                }
                try {
                    dataOperation = new DdlOperation(value);
                    sinkRecordObject.setDataOperation(dataOperation);
                    sinkRecordQueue.add(sinkRecordObject);
                }
                catch (DataException exception) {
                    dataOperation = new DmlOperation(value);
                    sinkRecordObject.setDataOperation(dataOperation);
                    sinkRecordsArrayList.add(sinkRecordObject);
                }
            }
        }
    }

    /**
     * Create work threads
     */
    public void createWorkThreads() {
        getTableSnapshot();
        constructSqlThread();
        transactionDispatcherThread();
    }

    private void receivedStartReplayFlag() {
        Scanner scanner = new Scanner(System.in);
        while (true) {
            if (scanner.hasNext()) {
                String flag = scanner.next();
                if (flag.equals("start")) {
                    LOGGER.info("Received start semaphore, and start to replay kafka records.");
                    break;
                }
            }
        }
    }

    private void transactionDispatcherThread() {
        new Thread(() -> {
            transactionDispatcher.dispatcher();
        }).start();
    }

    private void constructSqlThread() {
        new Thread(() -> {
            try {
                while (true) {
                    SinkRecordObject sinkRecordObject = sinkRecordQueue.take();
                    constructSql(sinkRecordObject);
                }
            }
            catch (InterruptedException exp) {
                LOGGER.error("Interrupted exception occurred", exp);
            }
        }).start();
    }

    private void constructSql(SinkRecordObject sinkRecordObject) {
        boolean isDml = sinkRecordObject.getDataOperation().getIsDml();
        if (isDml) {
            if (needStartReplayFlag) {
                receivedStartReplayFlag();
                needStartReplayFlag = false;
            }
            constructDml(sinkRecordObject);
        }
        else {
            constructDdl(sinkRecordObject);
        }
    }

    private void constructDdl(SinkRecordObject sinkRecordObject) {
        SourceField sourceField = sinkRecordObject.getSourceField();
        String currentGtid = sourceField.getGtid();
        if (currentGtid == null) {
            needStartReplayFlag = true;
            return;
        }
        if (needStartReplayFlag) {
            receivedStartReplayFlag();
            needStartReplayFlag = false;
        }
        transaction.setSourceField(sourceField);
        DdlOperation ddlOperation = (DdlOperation) sinkRecordObject.getDataOperation();
        String schemaName = sourceField.getDatabase();
        String tableName = sourceField.getTable();
        String tableFullName = schemaName + "." + tableName;
        String ddl = ddlOperation.getDdl();
        if (StringUtils.isNullOrEmpty(tableName)) {
            sqlList.add(ddl);
        }
        else {
            String modifiedDdl = ddl.replace("`", "").replaceFirst(tableName, tableFullName);
            sqlList.add(modifiedDdl);
            if (modifiedDdl.startsWith("alter table") || modifiedDdl.startsWith("create table")) {
                changedTableNameList.add(tableFullName);
            }
        }
        transaction.setSqlList(sqlList);
        transaction.setIsDml(false);
        transactionQueue.add(transaction.clone());
        sqlList.clear();
    }

    private void constructDml(SinkRecordObject sinkRecordObject) {
        SourceField sourceField = sinkRecordObject.getSourceField();
        String currentGtid = sourceField.getGtid();
        transaction.setSourceField(sourceField);
        transaction.setIsDml(true);

        DmlOperation dmlOperation = (DmlOperation) sinkRecordObject.getDataOperation();
        String operation = dmlOperation.getOperation();
        Envelope.Operation operationEnum = Envelope.Operation.forCode(operation);
        String sql = "";
        TableMetaData tableMetaData = null;
        String schemaName = transaction.getSourceField().getDatabase();
        String tableName = transaction.getSourceField().getTable();
        String tableFullName = schemaName + "." + tableName;

        if (changedTableNameList.contains(tableFullName)) {
            try {
                while (true) {
                    String table = feedBackQueue.take();
                    changedTableNameList.remove(table);
                    String[] schemaAndTable = table.split("\\.");
                    tableMetaData = sqlTools.getTableMetaData(schemaAndTable[0], schemaAndTable[1]);
                    tableMetaDataMap.put(table, tableMetaData);
                    if (table.equals(tableFullName) && !changedTableNameList.contains(tableFullName)) {
                        break;
                    }
                }
            }
            catch (InterruptedException exp) {
                LOGGER.error("Interrupted exception occurred", exp);
            }
        }
        else if (!tableMetaDataMap.containsKey(tableFullName)) {
            tableMetaData = sqlTools.getTableMetaData(schemaName, tableName);
            tableMetaDataMap.put(tableFullName, tableMetaData);
        }
        else {
            tableMetaData = tableMetaDataMap.get(tableFullName);
        }
        switch (operationEnum) {
            case CREATE:
                sql = sqlTools.getInsertSql(tableMetaData, dmlOperation.getAfter());
                break;
            case DELETE:
                sql = sqlTools.getDeleteSql(tableMetaData, dmlOperation.getBefore());
                break;
            case UPDATE:
                sql = sqlTools.getUpdateSql(tableMetaData, dmlOperation.getBefore(), dmlOperation.getAfter());
                break;
            default:
                break;
        }
        sqlList.add(sql);
        if (sqlList.size() == dmlEventCountMap.getOrDefault(currentGtid, (long) -1)) {
            dmlEventCountMap.remove(currentGtid);
            transaction.setSqlList(sqlList);
            transactionQueue.add(transaction.clone());
            sqlList.clear();
        }
    }

    private void getTableSnapshot() {
        Connection conn = openGaussConnection.createMysqlConnection();
        try {
            PreparedStatement ps = conn.prepareStatement("select v_schema_name, v_table_name, t_binlog_name," +
                    " i_binlog_position from sch_chameleon.t_replica_tables;");
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                String schemaName = rs.getString("v_schema_name");
                String tableName = rs.getString("v_table_name");
                String binlog_name = rs.getString("t_binlog_name");
                String binloig_position = rs.getString("i_binlog_position");
                tableSnapshotHashmap.put(schemaName + "." + tableName, binlog_name + ":" + binloig_position);
            }
        }
        catch (SQLException exp) {
            LOGGER.warn("sch_chameleon.t_replica_tables does not exist.", exp);
        }
    }

    private boolean isSkippedEvent(SourceField sourceField) {
        String schemaName = sourceField.getDatabase();
        String tableName = sourceField.getTable();
        String fullName = schemaName + "." + tableName;
        if (tableSnapshotHashmap.containsKey(fullName)) {
            String binlogFile = sourceField.getFile();
            Long fileIndex = Long.valueOf(binlogFile.split("\\.")[1]);
            Long binlogPosition = sourceField.getPosition();
            String snapshotPoint = tableSnapshotHashmap.get(fullName);
            String snapshotBinlogFile = snapshotPoint.split(":")[0];
            Long snapshotFileIndex = Long.valueOf(snapshotBinlogFile.split("\\.")[1]);
            Long snapshotBinligPosition = Long.valueOf(snapshotPoint.split(":")[1]);
            if (fileIndex < snapshotFileIndex ||
                    (fileIndex == snapshotFileIndex && binlogPosition <= snapshotBinligPosition)) {
                return true;
            }
        }
        return false;
    }
}
