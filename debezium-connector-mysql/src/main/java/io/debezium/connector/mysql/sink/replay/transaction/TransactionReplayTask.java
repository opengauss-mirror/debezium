/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.sink.replay.transaction;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mysql.cj.jdbc.AbandonedConnectionCleanupThread;

import io.debezium.ThreadExceptionHandler;
import io.debezium.connector.mysql.process.MysqlProcessCommitter;
import io.debezium.connector.mysql.process.MysqlSinkProcessInfo;
import io.debezium.connector.mysql.sink.object.ConnectionInfo;
import io.debezium.connector.mysql.sink.object.DataOperation;
import io.debezium.connector.mysql.sink.object.DdlOperation;
import io.debezium.connector.mysql.sink.object.DmlOperation;
import io.debezium.connector.mysql.sink.object.SinkRecordObject;
import io.debezium.connector.mysql.sink.object.SourceField;
import io.debezium.connector.mysql.sink.object.TableMetaData;
import io.debezium.connector.mysql.sink.object.Transaction;
import io.debezium.connector.mysql.sink.object.TransactionRecordField;
import io.debezium.connector.mysql.sink.replay.ReplayTask;
import io.debezium.connector.mysql.sink.task.MySqlSinkConnectorConfig;
import io.debezium.connector.mysql.sink.util.SqlTools;
import io.debezium.data.Envelope;
import io.debezium.enums.ErrorCode;

/**
 * Description: JdbcDbWriter
 *
 * @author douxin
 * @since 2022-10-31
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
    private static final int TASK_GRACEFUL_SHUTDOWN_TIME = 5;

    private int maxQueueSize;
    private double openFlowControlThreshold;
    private double closeFlowControlThreshold;

    private volatile AtomicBoolean isBlock = new AtomicBoolean(false);
    private TransactionDispatcher transactionDispatcher;
    private MySqlSinkConnectorConfig config;

    private int count = 0;
    private int queueIndex = 0;
    private Transaction transaction = new Transaction();
    private boolean isConnection = true;

    private HashMap<String, Long> dmlEventCountMap = new HashMap<String, Long>();
    private HashMap<String, TableMetaData> tableMetaDataMap = new HashMap<String, TableMetaData>();

    private ArrayList<SinkRecordObject> sinkRecordsArrayList = new ArrayList<>();
    private BlockingQueue<String> feedBackQueue = new LinkedBlockingQueue<>();
    private BlockingQueue<SinkRecord> sinkQueue = new LinkedBlockingQueue<>();
    private ArrayList<ConcurrentLinkedQueue<Transaction>> transactionQueueList = new ArrayList<>();
    private LinkedList<Long> sqlOffsets = new LinkedList<>();

    private final ThreadPoolExecutor threadPool = new ThreadPoolExecutor(3, 3, 100,
            TimeUnit.SECONDS, new LinkedBlockingQueue<>(3));
    private final DateTimeFormatter ofPattern = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    /**
     * Constructor
     *
     * @param config MySqlSinkConnectorConfig mysql sink connector config
     */
    public TransactionReplayTask(MySqlSinkConnectorConfig config) {
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
    @Override
    public void doStop() {
        transactionDispatcher.setIsStop(true);
        try {
            // ensure this batch data all replay, wait 4s
            TimeUnit.SECONDS.sleep(TASK_GRACEFUL_SHUTDOWN_TIME - 1);
            writeXlogResult();
        }
        catch (InterruptedException e) {
            LOGGER.error("{}Interrupt exception", ErrorCode.THREAD_INTERRUPTED_EXCEPTION);
        }
    }

    private void initObject(MySqlSinkConnectorConfig config) {
        this.config = config;
        initOpenGaussConnection(config);
        initSchemaMappingMap(config.schemaMappings);
        initTransactionQueueList(TRANSACTION_QUEUE_NUM);
        initSqlTools();
        initRecordBreakpoint(config);
        initTransactionDispatcher(config.parallelReplayThreadNum);
        initXlogLocation(config.xlogLocation);
        initFlowControl(config);
    }

    private void initOpenGaussConnection(MySqlSinkConnectorConfig config) {
        openGaussConnection = new ConnectionInfo(config, isConnectionAlive);
    }

    private void initTransactionQueueList(int num) {
        for (int i = 0; i < num; i++) {
            transactionQueueList.add(new ConcurrentLinkedQueue<Transaction>());
        }
    }

    private void initSqlTools() {
        sqlTools = new SqlTools(openGaussConnection);
    }

    private void initTransactionDispatcher(int threadNum) {
        if (threadNum > 0) {
            transactionDispatcher = new TransactionDispatcher(threadNum, openGaussConnection, transactionQueueList,
                    sqlTools, feedBackQueue);
        }
        else {
            transactionDispatcher = new TransactionDispatcher(openGaussConnection, transactionQueueList,
                    sqlTools, feedBackQueue);
        }
        transactionDispatcher.initProcessCommitter(config.getFailSqlPath(), config.getFileSizeLimit());
        transactionDispatcher.initBreakPointRecord(breakPointRecord, config.getBreakpointRepeatCountLimit());
    }

    private void initFlowControl(MySqlSinkConnectorConfig config) {
        maxQueueSize = config.maxQueueSize;
        openFlowControlThreshold = config.openFlowControlThreshold;
        closeFlowControlThreshold = config.closeFlowControlThreshold;
        monitorSinkQueueSize();
    }

    /**
     * Batch write
     *
     * @param records Collection<SinkRecord> the sink records
     */
    @Override
    public void batchWrite(Collection<SinkRecord> records) {
        // if addedQueueMap is empty, explain occurred first booting or breakpoint,
        // and if exist breakpoint info, mean breakpoint condition
        if (addedQueueMap.isEmpty() && breakPointRecord.isExists(records)) {
            LOGGER.warn("There is a breakpoint condition");
            transactionDispatcher.setIsBpCondition(true);
            Collection<SinkRecord> filteredRecords = breakPointRecord.readRecord(records);
            sinkQueue.addAll(filteredRecords);
        }
        else {
            sinkQueue.addAll(records);
        }
    }

    /**
     * Create work threads
     */
    @Override
    public void createWorkThreads() {
        getTableSnapshot();
        parseSinkRecordThread();
        transactionDispatcherThread();
        statTask();
        if (config.isCommitProcess()) {
            statCommit();
        }
        AbandonedConnectionCleanupThread.uncheckedShutdown();
    }

    /**
     * parse record
     */
    public void parseRecord() {
        Thread.currentThread().setUncaughtExceptionHandler(new ThreadExceptionHandler());
        int skipNum = 0;
        Struct value;
        SinkRecord sinkRecord = null;
        SourceField sourceField;
        SinkRecordObject sinkRecordObject;
        DataOperation dataOperation;
        while (isConnection) {
            try {
                sinkRecord = sinkQueue.take();
            }
            catch (InterruptedException e) {
                LOGGER.error("{}Interrupted exception occurred", ErrorCode.THREAD_INTERRUPTED_EXCEPTION, e);
            }
            if (addedQueueMap.containsKey(sinkRecord != null ? sinkRecord.kafkaOffset() : -1L)) {
                continue;
            }
            if (sinkRecord.value() instanceof Struct) {
                value = (Struct) sinkRecord.value();
            }
            else {
                value = null;
            }
            if (value == null) {
                // sink record of delete will bring a null record,the record offset add to sqlKafkaOffsets
                addReplayedOffset(sinkRecord.kafkaOffset());
                continue;
            }
            try {
                if (TransactionRecordField.BEGIN.equals(value.getString(TransactionRecordField.STATUS))) {
                    transaction.setTxnBeginOffset(sinkRecord.kafkaOffset());
                    sinkQueueFirstOffset = sinkRecord.kafkaOffset();
                    addedQueueMap.put(sinkRecord.kafkaOffset(),
                            value.getString(TransactionRecordField.ID));
                    sinkRecordsArrayList.clear();
                }
                else {
                    if (skipNum < value.getInt64(TransactionRecordField.EVENT_COUNT)) {
                        dmlEventCountMap.put(value.getString(TransactionRecordField.ID),
                                value.getInt64(TransactionRecordField.EVENT_COUNT) - skipNum);
                    }
                    addedQueueMap.put(sinkRecord.kafkaOffset(),
                            value.getString(TransactionRecordField.ID));
                    if (value.getInt64(TransactionRecordField.EVENT_COUNT) == 0) {
                        addReplayedOffset(sinkQueueFirstOffset);
                        addReplayedOffset(sinkRecord.kafkaOffset());
                        MysqlSinkProcessInfo.SINK_PROCESS_INFO.autoIncreaseExtractCount();
                        MysqlSinkProcessInfo.SINK_PROCESS_INFO.autoIncreaseSkippedExcludeEventCount();
                    }
                    if (skipNum == value.getInt64(TransactionRecordField.EVENT_COUNT) && skipNum > 0) {
                        addReplayedOffset(sinkQueueFirstOffset);
                        addReplayedOffset(sinkRecord.kafkaOffset());
                    }
                    transaction.setTxnEndOffset(sinkRecord.kafkaOffset());
                    try {
                        for (SinkRecordObject oneSinkRecordObject : sinkRecordsArrayList) {
                            constructDml(oneSinkRecordObject);
                        }
                        if (!isConnection) {
                            LOGGER.error("{}There is a connection problem with the openGauss,"
                                    + " check the database status or connection", ErrorCode.DB_CONNECTION_EXCEPTION);
                            MysqlSinkProcessInfo.SINK_PROCESS_INFO.autoIncreaseExtractCount();
                            count++;
                            transactionDispatcher.addFailTransaction();
                        }
                    }
                    catch (Exception e) {
                        LOGGER.error("{}The connector caught an exception that cannot be covered,"
                                + " the transaction constructed failed.", ErrorCode.UNKNOWN, e);
                        MysqlSinkProcessInfo.SINK_PROCESS_INFO.autoIncreaseExtractCount();
                        count++;
                        sqlList.clear();
                        transactionDispatcher.addFailTransaction();
                    }
                    if (skipNum > 0) {
                        LOGGER.warn("Transaction {} contains {} records, and skips {} records due to table snapshot",
                                value.get(TransactionRecordField.ID),
                                value.getInt64(TransactionRecordField.EVENT_COUNT),
                                skipNum);
                        skipNum = 0;
                    }
                }
            }
            catch (DataException exp) {
                sourceField = new SourceField(value);
                if (sourceField.isFullData()) {
                    continue;
                }
                if (isSkippedEvent(sourceField)) {
                    skipNum++;
                    MysqlSinkProcessInfo.SINK_PROCESS_INFO.autoIncreaseExtractCount();
                    MysqlSinkProcessInfo.SINK_PROCESS_INFO.autoIncreaseSkippedCount();
                    LOGGER.warn("Skip one record: {}", sourceField);
                    addReplayedOffset(sinkRecord.kafkaOffset());
                    continue;
                }
                sinkRecordObject = new SinkRecordObject();
                sinkRecordObject.setSourceField(sourceField);
                sinkRecordObject.setKafkaOffset(sinkRecord.kafkaOffset());
                addedQueueMap.put(sinkRecord.kafkaOffset(),
                        sourceField.getGtid());
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

    private void addReplayedOffset(Long offset) {
        breakPointRecord.getReplayedOffset().add(offset);
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

    private void constructDml(SinkRecordObject sinkRecordObject) {
        SourceField sourceField = sinkRecordObject.getSourceField();
        transaction.setSourceField(sourceField);
        transaction.setIsDml(true);
        DmlOperation dmlOperation = null;
        if (sinkRecordObject.getDataOperation() instanceof DmlOperation) {
            dmlOperation = (DmlOperation) sinkRecordObject.getDataOperation();
        }
        TableMetaData tableMetaData = null;
        String schemaName = transaction.getSourceField().getDatabase();
        String tableName = transaction.getSourceField().getTable();
        String tableFullName = getSinkSchema(schemaName) + "." + tableName;

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
                LOGGER.error("{}Interrupted exception occurred", ErrorCode.THREAD_INTERRUPTED_EXCEPTION, exp);
            }
        }
        else if (!tableMetaDataMap.containsKey(tableFullName)) {
            tableMetaData = sqlTools.getTableMetaData(getSinkSchema(schemaName), tableName);
            tableMetaDataMap.put(tableFullName, tableMetaData);
        }
        else {
            tableMetaData = tableMetaDataMap.get(tableFullName);
        }
        if (tableMetaData == null && !sqlTools.getIsConnection()) {
            isConnection = false;
            sqlList.clear();
            return;
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
        sqlOffsets.add(sinkRecordObject.getKafkaOffset());
        String currentGtid = sourceField.getGtid();
        if (currentGtid == null) {
            currentGtid = dmlOperation.getTransactionId();
        }
        if (sqlList.size() == dmlEventCountMap.getOrDefault(currentGtid, (long) -1)) {
            dmlEventCountMap.remove(currentGtid);
            transaction.setSqlList(sqlList);
            transaction.setSqlOffsets(sqlOffsets);
            splitTransactionQueue();
            sqlList.clear();
            sqlOffsets.clear();
        }
    }

    private void splitTransactionQueue() {
        count++;
        MysqlSinkProcessInfo.SINK_PROCESS_INFO.autoIncreaseExtractCount();
        transactionQueueList.get(queueIndex).add(transaction.clone());
        if (count % MAX_VALUE == 0) {
            queueIndex++;
            if (queueIndex % TRANSACTION_QUEUE_NUM == 0) {
                queueIndex = 0;
            }
        }
    }

    private void statTask() {
        final int[] previousCount = { count };
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
            MysqlProcessCommitter processCommitter = new MysqlProcessCommitter(config);
            processCommitter.commitSinkProcessInfo();
        });
    }

    private void monitorSinkQueueSize() {
        int openFlowControlQueueSize = (int) (openFlowControlThreshold * maxQueueSize);
        int closeFlowControlQueueSize = (int) (closeFlowControlThreshold * maxQueueSize);
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                Thread.currentThread().setName("timer-queue-size");
                int size = sinkQueue.size();
                int storeKafkaSize = breakPointRecord.getStoreKafkaQueueSize();
                if (size > openFlowControlQueueSize || storeKafkaSize > openFlowControlQueueSize) {
                    if (!isBlock.get()) {
                        LOGGER.warn("[start flow control] current isBlock is {}, queue size is {}, which is "
                                + "more than {} * {}, so open flow control",
                                isBlock, size, openFlowControlThreshold, maxQueueSize);
                    }
                    isBlock.set(true);
                }
                if (size < closeFlowControlQueueSize && storeKafkaSize < closeFlowControlQueueSize) {
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

    @Override
    protected void updateTransaction(SinkRecordObject sinkRecordObject) {
        transaction.setSourceField(sinkRecordObject.getSourceField());
        transaction.setSqlList(sqlList);
        transaction.setIsDml(false);
        transaction.setTxnBeginOffset(sinkRecordObject.getKafkaOffset());
        transaction.setTxnEndOffset(sinkRecordObject.getKafkaOffset());
        splitTransactionQueue();
        sqlList.clear();
    }

    @Override
    protected void updateChangedTables(String ddl, String newSchemaName, String tableName, SourceField sourceField) {
        if (SqlTools.isCreateOrAlterTableStatement(ddl)) {
            changedTableNameList.add(newSchemaName + "." + tableName);
            sourceField.setDatabase(newSchemaName);
        }
    }
}
