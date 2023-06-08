/**
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.opengauss.sink.replay;

import io.debezium.connector.opengauss.process.OgProcessCommitter;
import io.debezium.connector.opengauss.process.OgSinkProcessInfo;
import io.debezium.connector.opengauss.sink.object.DmlOperation;
import io.debezium.connector.opengauss.sink.object.ConnectionInfo;
import io.debezium.connector.opengauss.sink.object.SinkRecordObject;
import io.debezium.connector.opengauss.sink.object.SourceField;
import io.debezium.connector.opengauss.sink.task.OpengaussSinkConnectorConfig;
import io.debezium.connector.opengauss.sink.utils.SqlTools;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import java.util.List;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Description: JdbcDbWriter
 *
 * @author wangzhengyuan
 * @since 2022-11-20
 */
public class JdbcDbWriter {
    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcDbWriter.class);
    private long extractCount;
    private int threadCount;
    private int runCount;
    private ConnectionInfo mysqlConnection;
    private SqlTools sqlTools;
    private OgProcessCommitter failSqlCommitter;
    private ArrayList<WorkThread> threadList = new ArrayList<>();
    private final ThreadPoolExecutor threadPool = new ThreadPoolExecutor(4, 4, 100,
            TimeUnit.SECONDS, new LinkedBlockingQueue<>(4));
    private final DateTimeFormatter ofPattern = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    private BlockingQueue<SinkRecord> sinkQueue = new LinkedBlockingQueue<>();
    private Map<String, Integer> runnableMap = new HashMap<>();
    private Map<String, String> schemaMappingMap = new HashMap<>();
    private OpengaussSinkConnectorConfig config;

    /**
     * Constructor
     *
     * @param config OpengaussSinkConnectorConfig the config
     */
    public JdbcDbWriter(OpengaussSinkConnectorConfig config) {
        this.config = config;
        initSchemaMappingMap(config.schemaMappings);
        mysqlConnection = new ConnectionInfo(config.mysqlUrl, config.mysqlUsername, config.mysqlPassword, config.port);
        sqlTools = new SqlTools(mysqlConnection);
        this.threadCount = config.maxThreadCount;
        for (int i = 0; i < threadCount; i++) {
            WorkThread workThread = new WorkThread(schemaMappingMap, mysqlConnection, sqlTools, i);
            threadList.add(workThread);
        }
        this.failSqlCommitter = new OgProcessCommitter(config.getFailSqlPath(), config.getFileSizeLimit());
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
     * Batch write
     *
     * @param records Collection<SinkRecord> the records
     */
    public void batchWrite(Collection<SinkRecord> records) {
        sinkQueue.addAll(records);
    }

    /**
     * create work thread
     */
    public void createWorkThread() {
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
            } catch (InterruptedException e) {
                LOGGER.error("Interrupted exception occurred", e);
            }
            assert sinkRecord != null;
            if (sinkRecord.value() instanceof Struct) {
                value = (Struct) sinkRecord.value();
            } else {
                value = null;
            }
            if (value == null) {
                continue;
            }
            extractCount++;
            OgSinkProcessInfo.SINK_PROCESS_INFO.setExtractCount(extractCount);
            DmlOperation dmlOperation = new DmlOperation(value);
            SourceField sourceField = new SourceField(value);
            SinkRecordObject sinkRecordObject = new SinkRecordObject();
            sinkRecordObject.setDmlOperation(dmlOperation);
            sinkRecordObject.setSourceField(sourceField);
            String schemaName = sourceField.getSchema();
            String tableName = sourceField.getTable();
            String tableFullName = schemaMappingMap.get(schemaName) + "." + tableName;
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
        } else {
            workThread = threadList.get(runCount % threadCount);
            workThread.addData(sinkRecordObject);
        }
        runnableMap.put(tableFullName, runCount % threadCount);
        runCount++;
    }

    private int[] getSuccessAndFailCount() {
        int successCount = 0;
        int failCount = 0;
        for (WorkThread workThread : threadList) {
            successCount += workThread.getSuccessCount();
            failCount += workThread.getFailCount();
        }
        return new int[]{successCount, failCount, successCount + failCount};
    }

    private int getRelyIndex(String tableFullName, String schemaName) {
        Set<String> set = runnableMap.keySet();
        Iterator<String> iterator = set.iterator();
        while (iterator.hasNext()) {
            String oldTableName = iterator.next();
            if (!sqlTools.getRelyTableList(oldTableName, schemaName).contains(tableFullName)) {
                return -1;
            } else {
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
                } catch (InterruptedException exp) {
                    LOGGER.warn("Interrupted exception occurred", exp);
                }
            }
        });
    }

    private void statCommit() {
        threadPool.execute(() -> {
            OgProcessCommitter processCommitter = new OgProcessCommitter(config);
            processCommitter.commitSinkProcessInfo();
        });
    }

    private void statReplayTask() {
        threadPool.execute(() -> {
            while (true) {
                OgSinkProcessInfo.SINK_PROCESS_INFO.setSuccessCount(getSuccessAndFailCount()[0]);
                OgSinkProcessInfo.SINK_PROCESS_INFO.setFailCount(getSuccessAndFailCount()[1]);
                OgSinkProcessInfo.SINK_PROCESS_INFO.setReplayedCount(getSuccessAndFailCount()[2]);
                List<String> failSqlList = collectFailSql();
                if (failSqlList.size() > 0) {
                    commitFailSql(failSqlList);
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