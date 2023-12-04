/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.sink.replay.table;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.ArrayList;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.oracle.process.OracleProcessCommitter;
import io.debezium.connector.oracle.process.OracleSinkProcessInfo;
import io.debezium.connector.oracle.sink.object.ConnectionInfo;
import io.debezium.connector.oracle.sink.object.DmlOperation;
import io.debezium.connector.oracle.sink.object.SinkRecordObject;
import io.debezium.connector.oracle.sink.object.SourceField;
import io.debezium.connector.oracle.sink.replay.ReplayTask;
import io.debezium.connector.oracle.sink.task.OracleSinkConnectorConfig;
import io.debezium.connector.oracle.sink.util.SqlTools;

/**
 * Description: TableReplayTask
 *
 * @author gbase
 * @since 2023-07-28
 */
public class TableReplayTask extends ReplayTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(TableReplayTask.class);
    private long extractCount;
    private int threadCount;
    private int runCount;
    private ConnectionInfo openGaussConnection;
    private final SqlTools sqlTools;
    private OracleProcessCommitter failSqlCommitter;
    private final List<WorkThread> threadList = new ArrayList<>();
    private final ThreadPoolExecutor threadPool = new ThreadPoolExecutor(4, 4, 100,
            TimeUnit.SECONDS, new LinkedBlockingQueue<>(4));
    private final DateTimeFormatter ofPattern = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    private final Map<String, Integer> runnableMap = new HashMap<>();
    private final Map<String, String> schemaMappingMap = new HashMap<>();
    private OracleSinkConnectorConfig config;
    private volatile AtomicBoolean isSinkQueueBlock = new AtomicBoolean(false);
    private volatile AtomicBoolean isWorkQueueBlock = new AtomicBoolean(false);

    private int maxQueueSize;
    private double openFlowControlThreshold;
    private double closeFlowControlThreshold;

    /**
     * Constructor
     *
     * @param config OracleSinkConnectorConfig the config
     */
    public TableReplayTask(OracleSinkConnectorConfig config) {
        this.config = config;
        initSchemaMappingMap(config.schemaMappings);
        openGaussConnection = new ConnectionInfo(config.openGaussUrl, config.openGaussUsername,
                config.openGaussPassword);
        sqlTools = new SqlTools(openGaussConnection.createOpenGaussConnection(), this.schemaMappingMap);
        this.threadCount = config.parallelReplayThreadNum;
        for (int i = 0; i < threadCount; i++) {
            WorkThread workThread = new WorkThread(schemaMappingMap, openGaussConnection, sqlTools, i);
            threadList.add(workThread);
        }
        this.failSqlCommitter = new OracleProcessCommitter(config.getFailSqlPath(), config.getFileSizeLimit());
        initFlowControl(config);
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
                continue;
            }
            String[] schema = pair.split(":");
            if (schema.length == 2) {
                schemaMappingMap.put(schema[0].trim(), schema[1].trim());
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
        while (true) {
            try {
                sinkRecord = sinkQueue.take();
            }
            catch (InterruptedException e) {
                LOGGER.error("Interrupted exception occurred", e);
            }
            assert sinkRecord != null;
            if (sinkRecord.value() instanceof Struct) {
                value = (Struct) sinkRecord.value();
            }
            else {
                value = null;
            }
            if (value == null) {
                continue;
            }
            extractCount++;
            OracleSinkProcessInfo.SINK_PROCESS_INFO.setExtractCount(extractCount);
            DmlOperation dmlOperation = new DmlOperation(value);
            SourceField sourceField = new SourceField(value);
            SinkRecordObject sinkRecordObject = new SinkRecordObject();
            sinkRecordObject.setDataOperation(dmlOperation);
            sinkRecordObject.setSourceField(sourceField);
            String schemaName = sourceField.getSchema();
            String tableName = sourceField.getTable();
            String tableFullName = schemaMappingMap.get(schemaName) + "." + tableName;
            while (isWorkQueueBlock()) {
                try {
                    TimeUnit.MILLISECONDS.sleep(50);
                }
                catch (InterruptedException exp) {
                    LOGGER.warn("Receive interrupted exception while work queue block:{}", exp.getMessage());
                }
            }
            findProperWorkThread(tableFullName, sinkRecordObject, schemaMappingMap.get(schemaName));
        }
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
        int size;
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
            workThread.setFreeBlock(size < closeFlowControlQueueSize);
        }
        if (isFreeBlock() && isWorkQueueBlock()) {
            LOGGER.warn("[close flow control work queue] current isWorkQueueBlock is {}, all the queue size is "
                    + "less than {} * {}, so close flow control",
                    isWorkQueueBlock, closeFlowControlThreshold, maxQueueSize);
            isWorkQueueBlock.set(false);
        }
    }

    private boolean isFreeBlock() {
        for (WorkThread workThread : threadList) {
            if (!workThread.isFreeBlock()) {
                return false;
            }
        }
        return true;
    }

    private void initFlowControl(OracleSinkConnectorConfig config) {
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
        for (String oldTableName : set) {
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
                    TimeUnit.SECONDS.sleep(1);
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
            OracleProcessCommitter processCommitter = new OracleProcessCommitter(config);
            processCommitter.commitSinkProcessInfo();
        });
    }

    private void statReplayTask() {
        threadPool.execute(() -> {
            while (true) {
                OracleSinkProcessInfo.SINK_PROCESS_INFO.setSuccessCount(getSuccessAndFailCount()[0]);
                OracleSinkProcessInfo.SINK_PROCESS_INFO.setFailCount(getSuccessAndFailCount()[1]);
                OracleSinkProcessInfo.SINK_PROCESS_INFO.setReplayedCount(getSuccessAndFailCount()[2]);
                List<String> failSqlList = collectFailSql();
                if (failSqlList.size() > 0) {
                    commitFailSql(failSqlList);
                }
                try {
                    TimeUnit.SECONDS.sleep(1);
                }
                catch (InterruptedException e) {
                    LOGGER.error("Interrupted exception occurred while thread sleeping", e);
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
