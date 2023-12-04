/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.sink.replay.transaction;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import io.debezium.connector.oracle.sink.object.ConnectionInfo;
import io.debezium.connector.oracle.sink.object.Transaction;
import io.debezium.connector.oracle.sink.object.TableMetaData;
import io.debezium.connector.oracle.sink.object.TransactionRecordField;
import io.debezium.connector.oracle.sink.object.SourceField;
import io.debezium.connector.oracle.sink.object.DmlOperation;
import io.debezium.connector.oracle.sink.object.DdlOperation;
import io.debezium.connector.oracle.sink.object.TableChangesField;
import io.debezium.connector.oracle.sink.object.SinkRecordObject;
import io.debezium.connector.oracle.sink.object.DataOperation;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.oracle.process.OracleProcessCommitter;
import io.debezium.connector.oracle.process.OracleSinkProcessInfo;
import io.debezium.connector.oracle.sink.replay.ReplayTask;
import io.debezium.connector.oracle.sink.task.OracleSinkConnectorConfig;
import io.debezium.connector.oracle.sink.util.SqlTools;
import io.debezium.data.Envelope;

/**
 * Description: TransactionReplayTask
 *
 * @author gbase
 * @since 2023/07/28
 **/
public class TransactionReplayTask extends ReplayTask {
    /**
     * Transaction queue num
     */
    public static final int TRANSACTION_QUEUE_NUM = 5;

    /**
     * Max value for splitting transaction queue
     */
    public static final int MAX_VALUE = 50000;

    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionReplayTask.class);

    private int maxQueueSize;
    private double openFlowControlThreshold;
    private double closeFlowControlThreshold;

    private final AtomicBoolean isBlock = new AtomicBoolean(false);
    private ConnectionInfo openGaussConnection;
    private SqlTools sqlTools;
    private TransactionDispatcher transactionDispatcher;
    private OracleSinkConnectorConfig config;

    private int count = 0;
    private int queueIndex = 0;
    private long extractCount;
    private long skippedCount;
    private long skippedExcludeEventCount;
    private final ArrayList<String> sqlList = new ArrayList<>();
    private final Transaction transaction = new Transaction();
    private String xlogLocation;

    private final HashMap<String, Long> dmlEventCountMap = new HashMap<>();
    private final HashMap<String, TableMetaData> tableMetaDataMap = new HashMap<>();
    private final HashMap<String, String> schemaMappingMap = new HashMap<>();

    private final ArrayList<String> changedTableNameList = new ArrayList<>();
    private final ArrayList<SinkRecordObject> sinkRecordsArrayList = new ArrayList<>();
    private final BlockingQueue<String> feedBackQueue = new LinkedBlockingQueue<>();
    private final ArrayList<ConcurrentLinkedQueue<Transaction>> transactionQueueList = new ArrayList<>();

    private final ThreadPoolExecutor threadPool = new ThreadPoolExecutor(3, 3, 100,
            TimeUnit.SECONDS, new LinkedBlockingQueue<>(3));
    private final DateTimeFormatter ofPattern = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    /**
     * Constructor
     *
     * @param config OracleSinkConnectorConfig oracle sink connector config
     */
    public TransactionReplayTask(OracleSinkConnectorConfig config) {
        initObject(config);
    }

    /**
     * Is block
     *
     * @return boolean true if is block
     */
    public boolean isBlock() {
        return this.isBlock.get();
    }

    /**
     * Do stop
     */
    public void doStop() {
        String result = sqlTools.getXlogLocation();
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(xlogLocation))) {
            bw.write("xlog.location=" + result);
        }
        catch (IOException exp) {
            LOGGER.error("Fail to write xlog location {}", result);
        }
        sqlTools.closeConnection();
        LOGGER.info("Online migration from oracle to openGauss has gracefully stopped and current xlog"
                + "location in openGauss is {}", result);
    }

    private void initObject(OracleSinkConnectorConfig config) {
        this.config = config;
        initOpenGaussConnection(config);
        initSchemaMappingMap(config.schemaMappings);
        initTransactionQueueList(TRANSACTION_QUEUE_NUM);
        initSqlTools(this.schemaMappingMap);
        initTransactionDispatcher();
        initXlogLocation(config.xlogLocation);
        initFlowControl(config);
    }

    private void initOpenGaussConnection(OracleSinkConnectorConfig config) {
        openGaussConnection = new ConnectionInfo(config.openGaussUrl, config.openGaussUsername,
                config.openGaussPassword);
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
            transactionQueueList.add(new ConcurrentLinkedQueue<>());
        }
    }

    private void initSqlTools(Map<String, String> schemaMapping) {
        sqlTools = new SqlTools(openGaussConnection.createOpenGaussConnection(), schemaMapping);
    }

    private void initXlogLocation(String xlogLocation) {
        this.xlogLocation = xlogLocation;
    }

    private void initTransactionDispatcher() {
        transactionDispatcher = new TransactionDispatcher(openGaussConnection,
                transactionQueueList, feedBackQueue);
        transactionDispatcher.initProcessCommitter(config.getFailSqlPath(), config.getFileSizeLimit());
    }

    private void initFlowControl(OracleSinkConnectorConfig config) {
        maxQueueSize = config.maxQueueSize;
        openFlowControlThreshold = config.openFlowControlThreshold;
        closeFlowControlThreshold = config.closeFlowControlThreshold;
        monitorSinkQueueSize();
    }

    /**
     * Create work threads
     */
    @Override
    public void createWorkThreads() {
        parseSinkRecordThread();
        transactionDispatcherThread();
        statTask();
        if (config.isCommitProcess()) {
            statCommit();
        }
    }

    /**
     * parse record
     */
    public void parseRecord() {
        int skipNum = 0;
        Struct value;
        SinkRecord sinkRecord = null;
        SourceField sourceField;
        String snapshot;
        SinkRecordObject sinkRecordObject;
        DataOperation dataOperation;
        while (true) {
            try {
                sinkRecord = sinkQueue.take();
            }
            catch (InterruptedException e) {
                LOGGER.error("Interrupted exception occurred", e);
            }
            if (sinkRecord != null && sinkRecord.value() instanceof Struct) {
                value = (Struct) sinkRecord.value();
            }
            else {
                value = null;
            }

            if (value == null) {
                continue;
            }

            try {
                if (TransactionRecordField.BEGIN.equals(value.getString(TransactionRecordField.STATUS))) {
                    sinkRecordsArrayList.clear();
                }
                else {
                    Long eventCount = value.getInt64(TransactionRecordField.EVENT_COUNT);
                    String txId = value.getString(TransactionRecordField.ID);
                    if (skipNum < eventCount) {
                        dmlEventCountMap.put(txId, eventCount - skipNum);
                    }
                    if (eventCount == 0) {
                        statExtractCount();
                        skippedExcludeEventCount++;
                        OracleSinkProcessInfo.SINK_PROCESS_INFO.setSkippedExcludeEventCount(skippedExcludeEventCount);
                    }
                    try {
                        for (SinkRecordObject oneSinkRecordObject : sinkRecordsArrayList) {
                            constructDml(oneSinkRecordObject);
                        }
                    }
                    catch (Exception e) {
                        LOGGER.error("The connector caught an exception that cannot be covered,"
                                + " the transaction constructed failed.", e);
                        statExtractCount();
                        count++;
                        sqlList.clear();
                        transactionDispatcher.addFailTransaction();
                    }
                    if (skipNum > 0) {
                        LOGGER.warn("Transaction {} contains {} records, and skips {} records due to table snapshot",
                                txId, eventCount, skipNum);
                        skipNum = 0;
                    }
                }
            }
            catch (DataException exp) {
                sourceField = new SourceField(value);
                snapshot = sourceField.getSnapshot();
                if ("true".equals(snapshot) || "last".equals(snapshot)) {
                    continue;
                }
                if (isSkippedEvent(sourceField)) {
                    skipNum++;
                    skippedCount++;
                    statExtractCount();
                    OracleSinkProcessInfo.SINK_PROCESS_INFO.setSkippedCount(skippedCount);
                    LOGGER.warn("Skip one record: {}", sourceField);
                    continue;
                }
                sinkRecordObject = new SinkRecordObject();
                sinkRecordObject.setSourceField(sourceField);

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

    private void parseSinkRecordThread() {
        threadPool.execute(() -> {
            Thread.currentThread().setName("parse-sink-thread");
            parseRecord();
        });
    }

    private void transactionDispatcherThread() {
        threadPool.execute(() -> {
            Thread.currentThread().setName("txn-dispatcher-thread");
            transactionDispatcher.dispatcher();
        });
    }

    private void constructDdl(SinkRecordObject sinkRecordObject) {
        SourceField sourceField = sinkRecordObject.getSourceField();
        transaction.setSourceField(sourceField);
        DdlOperation ddlOperation;
        if (sinkRecordObject.getDataOperation() instanceof DdlOperation) {
            ddlOperation = (DdlOperation) sinkRecordObject.getDataOperation();
        }
        else {
            return;
        }
        String schemaName = sourceField.getSchema();
        String tableName = sourceField.getTable();

        TableChangesField tableChangesField = ddlOperation.getTableChangesField();
        String ddlType = tableChangesField.getType();
        String ddl = "";
        switch (ddlType) {
            case "CREATE":
                ddl = sqlTools.getCreateTableStatement(tableName, tableChangesField);
                break;
            case "ALTER":
                ddl = sqlTools.getAlterTableStatement(tableName, tableChangesField);
                break;
            case "DROP":
                ddl = sqlTools.getDropTableStatement(tableName);
                break;
            default:
                break;
        }

        if (ddl == null || ddl.isEmpty()) {
            ddl = ddlOperation.getDdl();
        }

        String newSchemaName = schemaMappingMap.getOrDefault(schemaName, schemaName);
        sqlList.add("set current_schema to " + SqlTools.addingQuote(newSchemaName) + ";");
        sqlList.add(ddl);
        if (SqlTools.isCreateOrAlterTableStatement(ddl)) {
            changedTableNameList.add(newSchemaName + "." + tableName);
        }
        transaction.setSqlList(sqlList);
        transaction.setIsDml(false);
        splitTransactionQueue();
        sqlList.clear();
    }

    private void constructDml(SinkRecordObject sinkRecordObject) {
        SourceField sourceField = sinkRecordObject.getSourceField();
        transaction.setSourceField(sourceField);
        transaction.setIsDml(true);

        DmlOperation dmlOperation;
        if (sinkRecordObject.getDataOperation() instanceof DmlOperation) {
            dmlOperation = (DmlOperation) sinkRecordObject.getDataOperation();
        }
        else {
            return;
        }

        TableMetaData tableMetaData = null;
        String schemaName = transaction.getSourceField().getSchema();
        String tableName = transaction.getSourceField().getTable();
        String tableFullName = schemaMappingMap.getOrDefault(schemaName, schemaName) + "." + tableName;

        if (changedTableNameList.contains(tableFullName)) {
            try {
                while (true) {
                    String table = feedBackQueue.take();
                    String[] schemaAndTable = table.split("\\.");
                    String newSchema = schemaMappingMap.getOrDefault(schemaAndTable[0], schemaAndTable[0]);
                    String mappingTable = newSchema + "." + schemaAndTable[1];
                    changedTableNameList.remove(mappingTable);
                    tableMetaData = sqlTools.getTableMetaData(newSchema, schemaAndTable[1]);
                    tableMetaDataMap.put(mappingTable, tableMetaData);
                    if (mappingTable.equals(tableFullName) && !changedTableNameList.contains(tableFullName)) {
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
        String operation = dmlOperation.getOperation();
        Envelope.Operation operationEnum = Envelope.Operation.forCode(operation);
        String sql = "";
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
        String currentTxId = sourceField.getTxId();
        if (currentTxId == null) {
            currentTxId = dmlOperation.getTransactionId();
        }
        if (sqlList.size() == dmlEventCountMap.getOrDefault(currentTxId, (long) -1)) {
            dmlEventCountMap.remove(currentTxId);
            transaction.setSqlList(sqlList);
            splitTransactionQueue();
            sqlList.clear();
        }
    }

    private void splitTransactionQueue() {
        count++;
        statExtractCount();
        transactionQueueList.get(queueIndex).add(transaction.clone());
        if (count % MAX_VALUE == 0) {
            queueIndex++;
            if (queueIndex % TRANSACTION_QUEUE_NUM == 0) {
                queueIndex = 0;
            }
        }
    }

    private boolean isSkippedEvent(SourceField sourceField) {
        Long snapshotScn = config.snapshotOffsetScn;
        if (snapshotScn == null) {
            return false;
        }

        long scn;
        try {
            scn = Long.parseLong(sourceField.getScn());
        }
        catch (NumberFormatException e) {
            return true;
        }

        if (scn <= snapshotScn) {
            String schemaName = sourceField.getSchema();
            String tableName = sourceField.getTable();
            String fullName = schemaMappingMap.getOrDefault(schemaName, schemaName) + "." + tableName;
            String skipInfo = String.format("Table %s snapshot is %s, current position is %s, which is less than "
                    + "snapshot, so skip the record.", fullName, snapshotScn, scn);
            LOGGER.warn(skipInfo);
            return true;
        }

        return false;
    }

    private void statTask() {
        final int[] previousCount = {count};
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                Thread.currentThread().setName("timer-construct-txn");
                LOGGER.warn("have constructed {} transaction, and current time is {}, and current speed is {}",
                        count, ofPattern.format(LocalDateTime.now()), count - previousCount[0]);
                previousCount[0] = count;
            }
        };
        Timer timer = new Timer();
        timer.schedule(task, 1000, 1000);
    }

    private void statCommit() {
        threadPool.execute(() -> {
            OracleProcessCommitter processCommitter = new OracleProcessCommitter(config);
            processCommitter.commitSinkProcessInfo();
        });
    }

    private void statExtractCount() {
        extractCount++;
        OracleSinkProcessInfo.SINK_PROCESS_INFO.setExtractCount(extractCount);
    }

    private void monitorSinkQueueSize() {
        int openFlowControlQueueSize = (int) (openFlowControlThreshold * maxQueueSize);
        int closeFlowControlQueueSize = (int) (closeFlowControlThreshold * maxQueueSize);
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                Thread.currentThread().setName("timer-queue-size");
                int size = sinkQueue.size();
                if (size > openFlowControlQueueSize) {
                    if (!isBlock.get()) {
                        LOGGER.warn("[start flow control] current isBlock is {}, queue size is {}, which is "
                                + "more than {} * {}, so open flow control",
                                isBlock, size, openFlowControlThreshold, maxQueueSize);
                    }
                    isBlock.set(true);
                }
                if (size < closeFlowControlQueueSize) {
                    if (isBlock.get()) {
                        LOGGER.warn("[close flow control] current isBlock is {}, queue size is {}, which is "
                                + "less than {} * {}, so close flow control",
                                isBlock, size, closeFlowControlThreshold, maxQueueSize);
                    }
                    isBlock.set(false);
                }
            }
        };
        Timer timer = new Timer();
        timer.schedule(task, 10, 20);
    }
}
