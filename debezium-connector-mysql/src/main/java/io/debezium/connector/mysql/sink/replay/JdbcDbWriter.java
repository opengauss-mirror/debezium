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
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

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
import io.debezium.connector.mysql.sink.object.TransactionRecordField;
import io.debezium.connector.mysql.sink.task.MySqlSinkConnectorConfig;
import io.debezium.connector.mysql.sink.util.SqlTools;
import io.debezium.data.Envelope;

/**
 * Description: JdbcDbWriter
 * @author douxin
 * @date 2022/10/31
 **/
public class JdbcDbWriter {
    /**
     * Transaction queue num
     */
    public static final int TRANSACTION_QUEUE_NUM = 5;

    /**
     * Max value for splitting transaction queue
     */
    public static final int MAX_VALUE = 50000;

    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcDbWriter.class);

    private ConnectionInfo openGaussConnection;
    private SqlTools sqlTools;
    private TransactionDispatcher transactionDispatcher;

    private int count = 0;
    private int queueIndex = 0;
    private ArrayList<String> sqlList = new ArrayList<>();
    private Transaction transaction = new Transaction();

    private HashMap<String, Long> dmlEventCountMap = new HashMap<String, Long>();
    private HashMap<String, String> tableSnapshotHashmap = new HashMap<>();
    private HashMap<String, TableMetaData> tableMetaDataMap = new HashMap<String, TableMetaData>();
    private HashMap<String, String> schemaMappingMap = new HashMap<>();

    private ArrayList<String> changedTableNameList = new ArrayList<>();
    private ArrayList<SinkRecordObject> sinkRecordsArrayList = new ArrayList<>();
    private BlockingQueue<String> feedBackQueue = new LinkedBlockingQueue<>();
    private BlockingQueue<SinkRecord> sinkQueue = new LinkedBlockingQueue<>();
    private ArrayList<ConcurrentLinkedQueue<Transaction>> transactionQueueList = new ArrayList<>();

    private final DateTimeFormatter ofPattern = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    /**
     * Constructor
     *
     * @param MySqlSinkConnectorConfig mysql sink connector config
     */
    public JdbcDbWriter(MySqlSinkConnectorConfig config) {
        initObject(config);
    }

    private void initObject(MySqlSinkConnectorConfig config) {
        initOpenGaussConnection(config);
        initSchemaMappingMap(config.schemaMappings);
        initTransactionQueueList(TRANSACTION_QUEUE_NUM);
        initSqlTools();
        initTransactionDispatcher(config.parallelReplayThreadNum);
    }

    private void initOpenGaussConnection(MySqlSinkConnectorConfig config) {
        openGaussConnection = new ConnectionInfo(config.openGaussUrl, config.openGaussUsername, config.openGaussPassword);
    }

    private void initSchemaMappingMap(String schemaMappings) {
        String[] pairs = schemaMappings.split(";");
        for (String pair : pairs) {
            String[] schema = pair.split(":");
            if (schema.length == 2) {
                schemaMappingMap.put(schema[0].trim(), schema[1].trim());
            }
        }
    }

    private void initTransactionQueueList(int num) {
        for (int i = 0; i < num; i++) {
            transactionQueueList.add(new ConcurrentLinkedQueue<Transaction>());
        }
    }

    private void initSqlTools() {
        sqlTools = new SqlTools(openGaussConnection.createOpenGaussConnection());
    }

    private void initTransactionDispatcher(int threadNum) {
        if (threadNum > 0) {
            transactionDispatcher = new TransactionDispatcher(threadNum, openGaussConnection, transactionQueueList, changedTableNameList, feedBackQueue);
        }
        else {
            transactionDispatcher = new TransactionDispatcher(openGaussConnection, transactionQueueList, changedTableNameList, feedBackQueue);
        }
    }

    /**
     * Batch write
     *
     * @param Collection<SinkRecord> the sink records
     */
    public void batchWrite(Collection<SinkRecord> records) {
        sinkQueue.addAll(records);
    }

    /**
     * Create work threads
     */
    public void createWorkThreads() {
        getTableSnapshot();
        parseSinkRecordThread();
        transactionDispatcherThread();
        statTask();
    }

    private void parseSinkRecordThread() {
        new Thread(() -> {
            Thread.currentThread().setName("parse-sink-thread");
            parseRecord();
        }).start();
    }

    public void parseRecord() {
        int skipNum = 0;
        Struct value = null;
        SinkRecord sinkRecord = null;
        while (true) {
            try {
                sinkRecord = sinkQueue.take();
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
            value = (Struct) sinkRecord.value();
            if (value == null) {
                continue;
            }
            try {
                if (TransactionRecordField.BEGIN.equals(value.getString(TransactionRecordField.STATUS))) {
                    sinkRecordsArrayList.clear();
                }
                else {
                    dmlEventCountMap.put(value.getString(TransactionRecordField.ID),
                            value.getInt64(TransactionRecordField.EVENT_COUNT) - skipNum);
                    for (SinkRecordObject sinkRecordObject : sinkRecordsArrayList) {
                        constructDml(sinkRecordObject);
                    }
                    if (skipNum > 0) {
                        String skipLog = String.format(Locale.ROOT, "Transaction %s contains %s records, and " +
                                "skips %s records because of table snapshot", value.get(TransactionRecordField.ID),
                                value.getInt64(TransactionRecordField.EVENT_COUNT), skipNum);
                        LOGGER.warn(skipLog);
                        skipNum = 0;
                    }
                }
            }
            catch (DataException exp) {
                SinkRecordObject sinkRecordObject = new SinkRecordObject();
                SourceField sourceField = new SourceField(value);
                if (sourceField.getGtid() == null) {
                    continue;
                }
                sinkRecordObject.setSourceField(sourceField);
                DataOperation dataOperation = null;
                if (isSkippedEvent(sourceField)) {
                    skipNum++;
                    LOGGER.warn("Skip one record: " + sinkRecordObject);
                    continue;
                }
                try {
                    dataOperation = new DmlOperation(value);
                    sinkRecordObject.setDataOperation(dataOperation);
                    sinkRecordsArrayList.add(sinkRecordObject);
                }
                catch (DataException exception) {
                    dataOperation = new DdlOperation(value);
                    sinkRecordObject.setDataOperation(dataOperation);
                    constructDdl(sinkRecordObject);
                }
            }
        }
    }

    private void transactionDispatcherThread() {
        new Thread(() -> {
            Thread.currentThread().setName("txn-dispatcher-thread");
            transactionDispatcher.dispatcher();
        }).start();
    }

    private void constructDdl(SinkRecordObject sinkRecordObject) {
        SourceField sourceField = sinkRecordObject.getSourceField();
        String currentGtid = sourceField.getGtid();
        transaction.setSourceField(sourceField);
        DdlOperation ddlOperation = (DdlOperation) sinkRecordObject.getDataOperation();
        String schemaName = sourceField.getDatabase();
        String tableName = sourceField.getTable();
        String newSchemaName = schemaMappingMap.getOrDefault(schemaName, schemaName);
        String ddl = ddlOperation.getDdl();
        sqlList.add("set current_schema to " + newSchemaName);
        if (StringUtils.isNullOrEmpty(tableName)) {
            sqlList.add(ddl);
        }
        else {
            String modifiedDdl = null;
            if (ddl.toLowerCase(Locale.ROOT).startsWith("alter table") &&
                    ddl.toLowerCase(Locale.ROOT).contains("rename to") &&
                    !ddl.toLowerCase(Locale.ROOT).contains("`rename to")) {
                int preIndex = ddl.toLowerCase(Locale.ROOT).indexOf("table");
                int postIndex = ddl.toLowerCase(Locale.ROOT).indexOf("rename");
                String oldFullName = ddl.substring(preIndex + 6, postIndex).trim();
                if (oldFullName.split("\\.").length == 2) {
                    String oldName = oldFullName.split("\\.")[1];
                    modifiedDdl = ddl.replaceFirst(oldFullName, oldName);
                }
                else {
                    modifiedDdl = ddl;
                }
            }
            else if (ddl.toLowerCase(Locale.ROOT).startsWith("drop table")) {
                modifiedDdl = ddl.replaceFirst(addingBackquote(schemaName) + ".", "");
            }
            else {
                modifiedDdl = ignoreSchemaName(ddl, schemaName, tableName);
            }
            sqlList.add(modifiedDdl);
            if (SqlTools.isCreateOrAlterTableStatement(ddl)) {
                changedTableNameList.add(newSchemaName + "." + tableName);
                transaction.getSourceField().setDatabase(newSchemaName);
            }
        }
        transaction.setSqlList(sqlList);
        transaction.setIsDml(false);
        splitTransactionQueue();
        sqlList.clear();
    }

    private String ignoreSchemaName(String ddl, String schemaName, String tableName) {
        Set<String> schemaTableSet = new HashSet<>();
        schemaTableSet.add(schemaName + "." + tableName);
        schemaTableSet.add(addingBackquote(schemaName) + "." + tableName);
        schemaTableSet.add(schemaName + "." + addingBackquote(tableName));
        schemaTableSet.add(addingBackquote(schemaName) + "." + addingBackquote(tableName));
        for (String name : schemaTableSet) {
            if (ddl.contains(name)) {
                return ddl.replaceFirst(name, addingBackquote(tableName));
            }
        }
        return ddl;
    }

    private String addingBackquote(String name) {
        return "`" + name + "`";
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
        String tableFullName = schemaMappingMap.getOrDefault(schemaName, schemaName) + "." + tableName;

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
            tableMetaData = sqlTools.getTableMetaData(schemaMappingMap.getOrDefault(schemaName, schemaName), tableName);
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
            splitTransactionQueue();
            sqlList.clear();
        }
    }

    private void splitTransactionQueue() {
        count++;
        transactionQueueList.get(queueIndex).add(transaction.clone());
        if (count % MAX_VALUE == 0) {
            queueIndex++;
            if (queueIndex % TRANSACTION_QUEUE_NUM == 0) {
                queueIndex = 0;
            }
        }
    }

    private void getTableSnapshot() {
        Connection conn = openGaussConnection.createOpenGaussConnection();
        try {
            PreparedStatement ps = conn.prepareStatement("select v_schema_name, v_table_name, t_binlog_name," +
                    " i_binlog_position from sch_chameleon.t_replica_tables;");
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                String schemaName = rs.getString("v_schema_name");
                String tableName = rs.getString("v_table_name");
                String binlog_name = rs.getString("t_binlog_name");
                String binlog_position = rs.getString("i_binlog_position");
                tableSnapshotHashmap.put(schemaMappingMap.getOrDefault(schemaName, schemaName) + "." + tableName,
                        binlog_name + ":" + binlog_position);
            }
        }
        catch (SQLException exp) {
            LOGGER.warn("sch_chameleon.t_replica_tables does not exist.");
        }
    }

    private boolean isSkippedEvent(SourceField sourceField) {
        String schemaName = sourceField.getDatabase();
        String tableName = sourceField.getTable();
        String fullName = schemaMappingMap.getOrDefault(schemaName, schemaName) + "." + tableName;
        if (tableSnapshotHashmap.containsKey(fullName)) {
            String binlogFile = sourceField.getFile();
            long fileIndex = Long.valueOf(binlogFile.split("\\.")[1]);
            long binlogPosition = sourceField.getPosition();
            String snapshotPoint = tableSnapshotHashmap.get(fullName);
            String snapshotBinlogFile = snapshotPoint.split(":")[0];
            long snapshotFileIndex = Long.valueOf(snapshotBinlogFile.split("\\.")[1]);
            long snapshotBinlogPosition = Long.valueOf(snapshotPoint.split(":")[1]);
            if (fileIndex < snapshotFileIndex ||
                    (fileIndex == snapshotFileIndex && binlogPosition <= snapshotBinlogPosition)) {
                String skipInfo = String.format("Table %s snapshot is %s, current position is %s, which is less than " +
                        "table snapshot, so skip the record.", fullName, snapshotPoint,
                        binlogFile + ":" + binlogPosition);
                LOGGER.warn(skipInfo);
                return true;
            }
        }
        return false;
    }

    private void statTask() {
        Timer timer = new Timer();
        final int[] before = { count };
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                String date = ofPattern.format(LocalDateTime.now());
                String result = String.format("have constructed %s transaction, and current time is %s, and current "
                        + "speed is %s", count, date, count - before[0]);
                LOGGER.warn(result);
                before[0] = count;
            }
        };
        timer.schedule(task, 1000, 1000);
    }
}
