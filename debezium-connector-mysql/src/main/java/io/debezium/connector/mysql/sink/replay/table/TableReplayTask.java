/**
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.sink.replay.table;

import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.mysql.process.MysqlProcessCommitter;
import io.debezium.connector.mysql.process.MysqlSinkProcessInfo;
import io.debezium.connector.mysql.sink.object.ConnectionInfo;
import io.debezium.connector.mysql.sink.object.DataOperation;
import io.debezium.connector.mysql.sink.object.DdlOperation;
import io.debezium.connector.mysql.sink.object.DmlOperation;
import io.debezium.connector.mysql.sink.object.SinkRecordObject;
import io.debezium.connector.mysql.sink.object.SourceField;
import io.debezium.connector.mysql.sink.object.TableMetaData;
import io.debezium.connector.mysql.sink.replay.ReplayTask;
import io.debezium.connector.mysql.sink.task.MySqlSinkConnectorConfig;
import io.debezium.connector.mysql.sink.util.SqlTools;
import io.debezium.data.Envelope;

/**
 * Description: JdbcDbWriter
 *
 * @author douxin
 * @since 2023-06-26
 */
public class TableReplayTask extends ReplayTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(TableReplayTask.class);

    private static final String INSERT = "c";
    private static final String UPDATE = "u";
    private static final String DELETE = "d";
    private static final int TASK_GRACEFUL_SHUTDOWN_TIME = 5;
    private static final int BREAKPOINT_REPEAT_COUNT_LIMIT = 3000;

    private int threadCount;
    private int runCount;
    private MysqlProcessCommitter failSqlCommitter;
    private ArrayList<WorkThread> threadList = new ArrayList<>();
    private boolean isStop = false;
    private final ThreadPoolExecutor threadPool = new ThreadPoolExecutor(4, 4, 100,
            TimeUnit.SECONDS, new LinkedBlockingQueue<>(4));
    private final DateTimeFormatter ofPattern = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    private final Map<String, TableMetaData> oldTableMap = new HashMap<>();

    private Map<String, Integer> runnableMap = new HashMap<>();
    private MySqlSinkConnectorConfig config;
    private volatile AtomicBoolean isSinkQueueBlock = new AtomicBoolean(false);
    private volatile AtomicBoolean isWorkQueueBlock = new AtomicBoolean(false);

    private int maxQueueSize;
    private double openFlowControlThreshold;
    private double closeFlowControlThreshold;
    private boolean isBpCondition = false;
    private int filterCount = 0;

    /**
     * Constructor
     *
     * @param config MySqlSinkConnectorConfig the config
     */
    public TableReplayTask(MySqlSinkConnectorConfig config) {
        this.config = config;
        initSchemaMappingMap(config.schemaMappings);
        initRecordBreakpoint(config);
        openGaussConnection = new ConnectionInfo(config.openGaussUrl, config.openGaussUsername,
                config.openGaussPassword);
        sqlTools = new SqlTools(openGaussConnection.createOpenGaussConnection());
        this.threadCount = config.parallelReplayThreadNum;
        for (int i = 0; i < threadCount; i++) {
            WorkThread workThread = new WorkThread(schemaMappingMap, openGaussConnection,
                    sqlTools, i, breakPointRecord);
            threadList.add(workThread);
        }
        this.failSqlCommitter = new MysqlProcessCommitter(config.getFailSqlPath(), config.getFileSizeLimit());
        initFlowControl(config);
        initXlogLocation(config.xlogLocation);
        printSinkRecordObject();
    }

    @Override
    public boolean isBlock() {
        return isSinkQueueBlock() || isWorkQueueBlock();
    }

    /**
     * Is block
     *
     * @return boolean true if is block
     */
    public boolean isSinkQueueBlock() {
        return this.isSinkQueueBlock.get();
    }

    /**
     * Get traffic limit flag
     *
     * @return boolean the traffic limit flag
     */
    public boolean isWorkQueueBlock() {
        return this.isWorkQueueBlock.get();
    }

    /**
     * Batch write
     *
     * @param records Collection<SinkRecord> the records
     */
    public void batchWrite(Collection<SinkRecord> records) {
        if (addedQueueMap.isEmpty() && breakPointRecord.isExists(records)) {
            LOGGER.info("There is a breakpoint condition");
            Collection<SinkRecord> filteredRecords = breakPointRecord.readRecord(records);
            // kill -9 or kafka shutdown occurred record already replayed,
            // but breakpoint not store the record
            isBpCondition = true;
            sinkQueue.addAll(filteredRecords);
        }
        else {
            sinkQueue.addAll(records);
        }
    }

    /**
     * Do stop
     */
    public void doStop() {
        isStop = true;
        for (WorkThread workThread : threadList) {
            workThread.setIsStop(isStop);
        }
        try {
            TimeUnit.SECONDS.sleep(TASK_GRACEFUL_SHUTDOWN_TIME - 1);
            writeXlogResult();
            closeConnection();
        }
        catch (InterruptedException exp) {
            LOGGER.error("Interrupt exception");
        }
    }

    private void closeConnection() {
        for (WorkThread workThread : threadList) {
            if (workThread.getConnection() != null) {
                try {
                    workThread.getConnection().close();
                }
                catch (SQLException exp) {
                    LOGGER.error("Unexpected error while closing the connection, the exception message is {}",
                            exp.getMessage());
                }
                finally {
                    workThread.setConnection(null);
                }
            }
        }
    }

    /**
     * create work thread
     */
    @Override
    public void createWorkThreads() {
        getTableSnapshot();
        parseSinkRecordThread();
        statTask();
        if (config.isCommitProcess()) {
            statCommit();
            statReplayTask();
        }
    }

    private void parseSinkRecordThread() {
        threadPool.execute(this::parseRecord);
    }

    /**
     * parse record
     */
    public void parseRecord() {
        SinkRecord sinkRecord = null;
        Struct value = null;
        while (!isStop) {
            try {
                sinkRecord = sinkQueue.take();
            }
            catch (InterruptedException e) {
                LOGGER.error("Interrupted exception occurred", e);
            }
            assert sinkRecord != null;
            if (addedQueueMap.containsKey(sinkRecord.kafkaOffset())) {
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
                breakPointRecord.getReplayedOffset().offer(sinkRecord.kafkaOffset());
                continue;
            }
            parseStructValue(sinkRecord, value);
        }
    }

    private void parseStructValue(SinkRecord sinkRecord, Struct value) {
        SourceField sourceField = new SourceField(value);
        if (sourceField.isFullData()) {
            return;
        }
        MysqlSinkProcessInfo.SINK_PROCESS_INFO.autoIncreaseExtractCount();
        if (isSkippedEvent(sourceField)) {
            MysqlSinkProcessInfo.SINK_PROCESS_INFO.autoIncreaseSkippedCount();
            LOGGER.warn("Skip one record: {}", sourceField);
            return;
        }
        DataOperation dataOperation;
        long kafkaOffset = sinkRecord.kafkaOffset();
        SinkRecordObject sinkRecordObject = new SinkRecordObject();
        sinkRecordObject.setKafkaOffset(kafkaOffset);
        String schemaName = sourceField.getDatabase();
        String gtid = sourceField.getGtid();
        if (filterByDb(sinkRecord, gtid, kafkaOffset)) {
            return;
        }
        addedQueueMap.put(sinkRecord.kafkaOffset(), gtid);
        if (!schemaMappingMap.containsKey(schemaName)) {
            LOGGER.error("schema mapping of source schema {} is not initialized, "
                    + "this ddl will be ignored.", schemaName);
            noSchemaCount++;
            return;
        }
        waitUnlockFlowControl();
        String tableName = sourceField.getTable();
        String tableFullName = getSinkSchema(schemaName) + "." + tableName;
        try {
            dataOperation = new DmlOperation(value);
            sinkRecordObject.setDataOperation(dataOperation);
            sinkRecordObject.setSourceField(sourceField);
        }
        catch (DataException exception) {
            dataOperation = new DdlOperation(value);
            sinkRecordObject.setDataOperation(dataOperation);
            sinkRecordObject.setSourceField(sourceField);
            constructDdl(sinkRecordObject);
            if (sqlList.size() > 0) {
                findDdlThread(tableFullName, sinkRecordObject);
            }
            return;
        }
        findProperWorkThread(tableFullName, sinkRecordObject);
    }

    private void waitUnlockFlowControl() {
        while (isWorkQueueBlock()) {
            try {
                Thread.sleep(50);
            }
            catch (InterruptedException exp) {
                LOGGER.warn("Receive interrupted exception while work queue block.", exp.getMessage());
            }
        }
    }

    private void findDdlThread(String tableFullName, SinkRecordObject sinkRecordObject) {
        sinkRecordObject.getDdlSqlList().addAll(sqlList);
        sinkRecordObject.getChangedTableList().addAll(changedTableNameList);
        sqlList.clear();
        changedTableNameList.clear();
        if ("".equals(primaryTable)) {
            findProperWorkThread(tableFullName, sinkRecordObject);
            return;
        }
        String tableName;
        if (runnableMap.containsKey(primaryTable)) {
            if (runnableMap.containsKey(tableFullName)
                    && !runnableMap.get(primaryTable).equals(runnableMap.get(tableFullName))) {
                mergeWorkQueue(tableFullName);
                tableName = primaryTable;
            }
            else {
                runnableMap.put(tableFullName, runnableMap.get(primaryTable));
                tableName = tableFullName;
            }
        }
        else if (runnableMap.containsKey(tableFullName)) {
            runnableMap.put(primaryTable, runnableMap.get(tableFullName));
            tableName = primaryTable;
        }
        else {
            findProperWorkThread(primaryTable, null);
            runnableMap.put(tableFullName, runnableMap.get(primaryTable));
            tableName = tableFullName;
        }
        findProperWorkThread(tableName, sinkRecordObject);
        primaryTable = "";
    }

    private void mergeWorkQueue(String tableFullName) {
        WorkThread workThread = threadList.get(runnableMap.get(tableFullName));
        int index = runnableMap.get(primaryTable);
        WorkThread targetThread = threadList.get(index);
        for (String tableName : workThread.getTableSet()) {
            runnableMap.remove(tableName);
            runnableMap.put(tableName, index);
        }
        targetThread.addWorkData(workThread.getSinkRecordQueue(), workThread.getTableSet());
        workThread.removeWorkData();
    }

    private boolean filterByDb(SinkRecord sinkRecord, String gtid, long kafkaOffset) {
        if (isBpCondition && filterCount < BREAKPOINT_REPEAT_COUNT_LIMIT) {
            filterCount++;
            if (isSkipRecord(sinkRecord)) {
                LOGGER.info("The sinkRecord is already replay,"
                        + " so skip this txn that gtid is {}", gtid);
                breakPointRecord.getReplayedOffset().add(kafkaOffset);
                return true;
            }
        }
        return false;
    }

    private boolean isSkipRecord(SinkRecord sinkRecord) {
        Struct value;
        if (sinkRecord.value() instanceof Struct) {
            value = (Struct) sinkRecord.value();
        }
        else {
            value = null;
        }
        if (value == null) {
            return false;
        }
        DmlOperation dmlOperation = new DmlOperation(value);
        SourceField sourceField = new SourceField(value);
        String schemaName = getSinkSchema(sourceField.getDatabase());
        String operation = dmlOperation.getOperation();
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
        List<String> sqlList;
        switch (operation) {
            case INSERT:
                sql = sqlTools.getReadSql(tableMetaData, dmlOperation.getAfter(), Envelope.Operation.CREATE);
                return sqlTools.isExistSql(sql);
            case UPDATE:
                sqlList = sqlTools.getReadSqlForUpdate(tableMetaData, dmlOperation.getBefore(),
                        dmlOperation.getAfter());
                if (sqlList.size() == 1) {
                    return sqlTools.isExistSql(sqlList.get(0));
                }
                else if (sqlList.size() == 2) {
                    return sqlTools.isExistSql(sqlList.get(0)) && !sqlTools.isExistSql(sqlList.get(1));
                }
                else {
                    return false;
                }
            case DELETE:
                sql = sqlTools.getReadSql(tableMetaData, dmlOperation.getBefore(), Envelope.Operation.DELETE);
                return !sqlTools.isExistSql(sql);
            default:
                return false;
        }
    }

    private void findProperWorkThread(String tableFullName, SinkRecordObject sinkRecordObject) {
        if (runnableMap.containsKey(tableFullName)) {
            WorkThread workThread = threadList.get(runnableMap.get(tableFullName));
            workThread.addData(sinkRecordObject, tableFullName);
            return;
        }
        int relyThreadIndex = getRelyIndex(tableFullName);
        if (relyThreadIndex != -1) {
            WorkThread workThread = threadList.get(relyThreadIndex);
            workThread.addData(sinkRecordObject, tableFullName);
            runnableMap.put(tableFullName, relyThreadIndex);
            return;
        }
        WorkThread workThread;
        if (runCount < threadCount) {
            workThread = threadList.get(runCount);
            workThread.addData(sinkRecordObject, tableFullName);
            workThread.start();
        }
        else {
            workThread = threadList.get(runCount % threadCount);
            workThread.addData(sinkRecordObject, tableFullName);
        }
        runnableMap.put(tableFullName, runCount % threadCount);
        runCount++;
    }

    // Flow control according to sink queue and work thread queue
    private void monitorSinkQueueSize() {
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                Thread.currentThread().setName("timer-sink-queue-size");
                getSinkQueueBlockFlag();
                getWorkThreadQueueFlag();
            }
        };
        Timer timer = new Timer();
        timer.schedule(task, 10, 20);
    }

    private void printSinkRecordObject() {
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                Thread.currentThread().setName("print-sink-record");
                for (WorkThread workThread : threadList) {
                    SinkRecordObject sinkRecordObject = workThread.getThreadSinkRecordObject();
                    if (sinkRecordObject != null) {
                        LOGGER.warn("[Breakpoint] {} in work thread {}",
                                sinkRecordObject.getSourceField().toString(), workThread.getName());
                    }
                }
            }
        };
        Timer timer = new Timer();
        timer.schedule(task, 1000, 1000 * 60 * 5);
    }

    private void getSinkQueueBlockFlag() {
        int openFlowControlQueueSize = (int) (openFlowControlThreshold * maxQueueSize);
        int closeFlowControlQueueSize = (int) (closeFlowControlThreshold * maxQueueSize);
        int size = sinkQueue.size();
        int storeKafkaSize = breakPointRecord.getStoreKafkaQueueSize();
        if (size > openFlowControlQueueSize || storeKafkaSize > openFlowControlQueueSize) {
            if (!isSinkQueueBlock.get()) {
                LOGGER.warn("[start flow control sink queue] current isSinkQueueBlock is {}, queue size is {}, which is "
                        + "more than {} * {}, so open flow control",
                        isSinkQueueBlock, size, openFlowControlThreshold, maxQueueSize);
                isSinkQueueBlock.set(true);
            }
        }
        if (size < closeFlowControlQueueSize && storeKafkaSize < closeFlowControlQueueSize) {
            if (isSinkQueueBlock.get()) {
                LOGGER.warn("[close flow control sink queue] current isSinkQueueBlock is {}, queue size is {}, which is "
                        + "less than {} * {}, so close flow control",
                        isSinkQueueBlock, size, closeFlowControlThreshold, maxQueueSize);
                isSinkQueueBlock.set(false);
            }
        }
    }

    private void getWorkThreadQueueFlag() {
        int openFlowControlQueueSize = (int) (openFlowControlThreshold * maxQueueSize);
        int closeFlowControlQueueSize = (int) (closeFlowControlThreshold * maxQueueSize);
        boolean freeBlock = false;
        int size = 0;
        for (WorkThread workThread : threadList) {
            size = workThread.getQueueLength();
            if (size > openFlowControlQueueSize) {
                if (!isWorkQueueBlock.get()) {
                    LOGGER.warn("[start flow control work queue] current isWorkQueueBlock is {}, queue size is {}, which is "
                            + "more than {} * {}, so open flow control",
                            isWorkQueueBlock, size, openFlowControlThreshold, maxQueueSize);
                    isWorkQueueBlock.set(true);
                    return;
                }
            }
            if (size < closeFlowControlQueueSize) {
                freeBlock = true;
            }
            else {
                freeBlock = false;
            }
        }
        if (freeBlock && isWorkQueueBlock()) {
            LOGGER.warn("[close flow control work queue] current isWorkQueueBlock is {}, all the queue size is "
                    + "less than {} * {}, so close flow control",
                    isWorkQueueBlock, closeFlowControlThreshold, maxQueueSize);
            isWorkQueueBlock.set(false);
        }
    }

    private void initFlowControl(MySqlSinkConnectorConfig config) {
        maxQueueSize = config.maxQueueSize;
        openFlowControlThreshold = config.openFlowControlThreshold;
        closeFlowControlThreshold = config.closeFlowControlThreshold;
        monitorSinkQueueSize();
    }

    private int[] getSuccessAndFailCount() {
        int successCount = 0;
        int failCount = noSchemaCount;
        for (WorkThread workThread : threadList) {
            successCount += workThread.getSuccessCount();
            failCount += workThread.getFailCount();
        }
        return new int[]{ successCount, failCount, successCount + failCount };
    }

    private int getRelyIndex(String currentTableName) {
        Set<String> set = runnableMap.keySet();
        Iterator<String> iterator = set.iterator();
        while (iterator.hasNext()) {
            String previousTableName = iterator.next();
            if (sqlTools.getForeignTableList(previousTableName).contains(currentTableName)
                    || sqlTools.getForeignTableList(currentTableName).contains(previousTableName)) {
                return runnableMap.get(previousTableName);
            }
        }
        return -1;
    }

    private void statTask() {
        threadPool.execute(() -> {
            int before = getSuccessAndFailCount()[2];
            while (true) {
                try {
                    Thread.sleep(1000);
                    if (LOGGER.isWarnEnabled()) {
                        LOGGER.warn("have replayed {} data, and current time is {}, and current "
                                + "speed is {}", getSuccessAndFailCount()[2],
                                ofPattern.format(LocalDateTime.now()),
                                getSuccessAndFailCount()[2] - before);
                    }
                    before = getSuccessAndFailCount()[2];
                }
                catch (InterruptedException exp) {
                    LOGGER.warn("Interrupted exception occurred", exp);
                }
            }
        });
    }

    private void statCommit() {
        threadPool.execute(() -> {
            MysqlProcessCommitter processCommitter = new MysqlProcessCommitter(config);
            processCommitter.commitSinkProcessInfo();
        });
    }

    private void statReplayTask() {
        threadPool.execute(() -> {
            while (true) {
                MysqlSinkProcessInfo.SINK_PROCESS_INFO.setSuccessCount(getSuccessAndFailCount()[0]);
                MysqlSinkProcessInfo.SINK_PROCESS_INFO.setFailCount(getSuccessAndFailCount()[1]);
                MysqlSinkProcessInfo.SINK_PROCESS_INFO.setReplayedCount(getSuccessAndFailCount()[2]);
                List<String> failSqlList = collectFailSql();
                if (failSqlList.size() > 0) {
                    commitFailSql(failSqlList);
                }
                try {
                    Thread.sleep(1000);
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    private List<String> collectFailSql() {
        List<String> failSqlList = new ArrayList<>();
        for (WorkThread workThread : threadList) {
            if (workThread.getFailSqlList().size() != 0) {
                failSqlList.addAll(workThread.getFailSqlList());
                workThread.clearFailSqlList();
            }
        }
        return failSqlList;
    }

    private void commitFailSql(List<String> failSqlList) {
        for (String sql : failSqlList) {
            failSqlCommitter.commitFailSql(sql);
        }
    }
}
