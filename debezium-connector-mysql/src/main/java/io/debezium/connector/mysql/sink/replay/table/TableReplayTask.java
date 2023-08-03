/**
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.sink.replay.table;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import io.debezium.config.Configuration;
import io.debezium.connector.breakpoint.BreakPointRecord;
import io.debezium.connector.mysql.sink.object.TableMetaData;
import io.debezium.data.Envelope;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.mysql.process.MysqlProcessCommitter;
import io.debezium.connector.mysql.process.MysqlSinkProcessInfo;
import io.debezium.connector.mysql.sink.object.ConnectionInfo;
import io.debezium.connector.mysql.sink.object.DmlOperation;
import io.debezium.connector.mysql.sink.object.SinkRecordObject;
import io.debezium.connector.mysql.sink.object.SourceField;
import io.debezium.connector.mysql.sink.replay.ReplayTask;
import io.debezium.connector.mysql.sink.task.MySqlSinkConnectorConfig;
import io.debezium.connector.mysql.sink.util.SqlTools;

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

    private long extractCount;
    private int threadCount;
    private int runCount;
    private ConnectionInfo openGaussConnection;
    private SqlTools sqlTools;
    private MysqlProcessCommitter failSqlCommitter;
    private BreakPointRecord breakPointRecord;
    private ArrayList<WorkThread> threadList = new ArrayList<>();
    private List<Long> toDeleteOffsets;
    private boolean isStop = false;
    private String xlogLocation;
    private final ThreadPoolExecutor threadPool = new ThreadPoolExecutor(4, 4, 100,
            TimeUnit.SECONDS, new LinkedBlockingQueue<>(4));
    private final DateTimeFormatter ofPattern = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    private final Map<String, TableMetaData> oldTableMap = new HashMap<>();

    private Map<String, Integer> runnableMap = new HashMap<>();
    private Map<String, String> schemaMappingMap = new HashMap<>();
    private Map<Long, Long> addedQueueMap = new ConcurrentHashMap<>();
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

    private void initSchemaMappingMap(String schemaMappings) {
        String[] pairs = schemaMappings.split(";");
        for (String pair : pairs) {
            if (pair == null || " ".equals(pair)) {
                LOGGER.error("the format of schema.mappings is error:" + schemaMappings);
            }
            String[] schema = pair.split(":");
            if (schema.length == 2) {
                schemaMappingMap.put(schema[0].trim(), schema[1].trim());
            }
        }
    }

    /**
     * Init breakpoint record properties
     *
     * @param config OpengaussSinkConnectorConfig openGauss sink connector config
     */
    private void initRecordBreakpoint(MySqlSinkConnectorConfig config) {
        // properties configuration
        Configuration configuration = Configuration.create()
                .with(BreakPointRecord.BOOTSTRAP_SERVERS, config.getBootstrapServers())
                .with(BreakPointRecord.TOPIC, config.getBpTopic())
                .with(BreakPointRecord.RECOVERY_POLL_ATTEMPTS, config.getBpMaxRetries())
                .with(BreakPointRecord.RECOVERY_POLL_INTERVAL_MS, 500)
                .with(BreakPointRecord.consumerConfigPropertyName(
                        ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG),
                        100)
                .with(BreakPointRecord.consumerConfigPropertyName(
                        ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG),
                        50000)
                .build();
        breakPointRecord = new BreakPointRecord(configuration);
        toDeleteOffsets = breakPointRecord.getToDeleteOffsets();
        breakPointRecord.setBpQueueTimeLimit(config.getBpQueueTimeLimit());
        breakPointRecord.setBpQueueSizeLimit(config.getBpQueueSizeLimit());
        breakPointRecord.start();
        if (!breakPointRecord.isTopicExist()) {
            breakPointRecord.initializeStorage();
        }
    }

    private void initXlogLocation(String xlogLocation) {
        this.xlogLocation = xlogLocation;
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
        } else {
            sinkQueue.addAll(records);
        }
    }

    /**
     * Do stop
     */
    public void doStop() {
        isStop = true;
        try {
            TimeUnit.SECONDS.sleep(TASK_GRACEFUL_SHUTDOWN_TIME - 1);
            writeXlogResult();
            closeConnection();
        } catch (InterruptedException exp) {
            LOGGER.error("Interrupt exception");
        }
    }

    private void writeXlogResult() {
        String xlogResult = sqlTools.getXlogLocation();
        sqlTools.closeConnection();
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(xlogLocation))) {
            bw.write("xlog.location=" + xlogResult);
        } catch (IOException exp) {
            LOGGER.error("Fail to write xlog location {}", xlogResult);
        }
        LOGGER.info("Online migration from mysql to openGauss has gracefully stopped and current xlog"
                + "location in openGauss is {}", xlogResult);
    }

    private void closeConnection() {
        for (WorkThread workThread : threadList) {
            try {
                workThread.getConnection().close();
            } catch (SQLException exp) {
                LOGGER.error("Unexpected error while closing the connection, the exception message is {}",
                        exp.getMessage());
            } finally {
                workThread.setConnection(null);
            }
        }
    }

    /**
     * create work thread
     */
    @Override
    public void createWorkThreads() {
        parseSinkRecordThread();
        statTask();
        if (config.isCommitProcess()) {
            statCommit();
        }
        statReplayTask();
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
            extractCount++;
            MysqlSinkProcessInfo.SINK_PROCESS_INFO.setExtractCount(extractCount);
            DmlOperation dmlOperation = new DmlOperation(value);
            SourceField sourceField = new SourceField(value);
            long kafkaOffset = sinkRecord.kafkaOffset();
            SinkRecordObject sinkRecordObject = new SinkRecordObject();
            sinkRecordObject.setDataOperation(dmlOperation);
            sinkRecordObject.setSourceField(sourceField);
            sinkRecordObject.setKafkaOffset(kafkaOffset);
            String schemaName = sourceField.getDatabase();
            String tableName = sourceField.getTable();
            String gtid = sourceField.getGtid();
            filterByDb(sinkRecord, gtid, kafkaOffset);
            String tableFullName = schemaMappingMap.get(schemaName) + "." + tableName;
            while (isWorkQueueBlock()) {
                try {
                    Thread.sleep(50);
                }
                catch (InterruptedException exp) {
                    LOGGER.warn("Receive interrupted exception while work queue block.", exp.getMessage());
                }
            }
            findProperWorkThread(tableFullName, sinkRecordObject, schemaMappingMap.get(schemaName));
        }
    }

    private void filterByDb(SinkRecord sinkRecord, String gtid, long kafkaOffset) {
        if (isBpCondition && filterCount < BREAKPOINT_REPEAT_COUNT_LIMIT) {
            filterCount++;
            if (isSkipRecord(sinkRecord)) {
                LOGGER.info("The sinkRecord is already replay,"
                        + " so skip this txn that gtid is {}", gtid);
                breakPointRecord.getReplayedOffset().add(kafkaOffset);
                return;
            }
        }
    }

    private boolean isSkipRecord(SinkRecord sinkRecord) {
        Struct value;
        if (sinkRecord.value() instanceof Struct) {
            value = (Struct) sinkRecord.value();
        } else {
            value = null;
        }
        if (value == null) {
            return false;
        }
        DmlOperation dmlOperation = new DmlOperation(value);
        SourceField sourceField = new SourceField(value);
        String schemaName = schemaMappingMap.get(sourceField.getDatabase());
        String operation = dmlOperation.getOperation();
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
            case UPDATE:
                sql = sqlTools.getReadSql(tableMetaData, dmlOperation.getAfter(), Envelope.Operation.CREATE);
                return sqlTools.isExistSql(sql);
            case DELETE:
                sql = sqlTools.getReadSql(tableMetaData, dmlOperation.getBefore(), Envelope.Operation.DELETE);
                return !sqlTools.isExistSql(sql);
            default:
                return false;
        }
    }

    /**
     * Get the offset of already replayed record
     *
     * @return the continuous and maximum offset
     */
    public Long getReplayedOffset() {
        PriorityBlockingQueue<Long> replayedOffsets = breakPointRecord.getReplayedOffset();
        Long offset = replayedOffsets.peek();
        Long endOffset = -1L;
        boolean isContinuous = true;
        while (isContinuous && !replayedOffsets.isEmpty()) {
            Long num;
            try {
                num = replayedOffsets.take();
                if (num.equals(offset)) {
                    endOffset = num;
                } else {
                    replayedOffsets.offer(num);
                    isContinuous = false;
                }
                offset++;
            } catch (InterruptedException exp) {
                LOGGER.error("Interrupted exception occurred", exp);
            }
        }
        if (endOffset == -1L) {
            return endOffset;
        }
        replayedOffsets.offer(endOffset);
        Iterator<Map.Entry<Long, Long>> iterator = addedQueueMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, Long> entry = iterator.next();
            if (entry.getKey() < endOffset) {
                iterator.remove();
            }
        }
        toDeleteOffsets.add(endOffset);
        return endOffset + 1;
    }

    private void findProperWorkThread(String tableFullName, SinkRecordObject sinkRecordObject, String schemaName) {
        if (runnableMap.containsKey(tableFullName)) {
            WorkThread workThread = threadList.get(runnableMap.get(tableFullName));
            workThread.addData(sinkRecordObject);
            return;
        }
        int relyThreadIndex = getRelyIndex(tableFullName, schemaName);
        if (relyThreadIndex != -1) {
            WorkThread workThread = threadList.get(relyThreadIndex);
            workThread.addData(sinkRecordObject);
            runnableMap.put(tableFullName, relyThreadIndex);
            return;
        }
        WorkThread workThread;
        if (runCount < threadCount) {
            workThread = threadList.get(runCount);
            workThread.addData(sinkRecordObject);
            workThread.start();
        }
        else {
            workThread = threadList.get(runCount % threadCount);
            workThread.addData(sinkRecordObject);
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
                        LOGGER.error("[Breakpoint] {} in work thread {}",
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
        if (size > openFlowControlQueueSize) {
            if (!isSinkQueueBlock.get()) {
                LOGGER.warn("[start flow control sink queue] current isSinkQueueBlock is {}, queue size is {}, which is "
                                + "more than {} * {}, so open flow control",
                        isSinkQueueBlock, size, openFlowControlThreshold, maxQueueSize);
                isSinkQueueBlock.set(true);
            }
        }
        if (size < closeFlowControlQueueSize) {
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
        int failCount = 0;
        for (WorkThread workThread : threadList) {
            successCount += workThread.getSuccessCount();
            failCount += workThread.getFailCount();
        }
        return new int[]{ successCount, failCount, successCount + failCount };
    }

    private int getRelyIndex(String tableFullName, String schemaName) {
        Set<String> set = runnableMap.keySet();
        Iterator<String> iterator = set.iterator();
        while (iterator.hasNext()) {
            String oldTableName = iterator.next();
            if (!sqlTools.getRelyTableList(oldTableName, schemaName).contains(tableFullName)) {
                return -1;
            }
            else {
                return runnableMap.get(oldTableName);
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
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("have replayed {} data, and current time is {}, and current "
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
