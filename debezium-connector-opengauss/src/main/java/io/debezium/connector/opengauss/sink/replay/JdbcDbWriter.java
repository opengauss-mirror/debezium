/**
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.opengauss.sink.replay;

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

import java.sql.Connection;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Description: JdbcDbWriter
 * @author wangzhengyuan
 * @date 2022/11/20
 */
public class JdbcDbWriter {
    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcDbWriter.class);
    private int threadCount;
    private int runCount;
    private ConnectionInfo mysqlConnection;
    private SqlTools sqlTools;
    private ArrayList<WorkThread> threadList = new ArrayList<>();
    private final DateTimeFormatter ofPattern = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    private BlockingQueue<SinkRecord> sinkQueue = new LinkedBlockingQueue<>();
    private static Map<String, Integer> runnableMap = new HashMap<>();
    private Map<String, String> schemaMappingMap = new HashMap<>();

    /**
     * Constructor
     * @param config OpengaussSinkConnectorConfig the config
     */
    public JdbcDbWriter(OpengaussSinkConnectorConfig config) {
        initSchemaMappingMap(config.schemaMappings);
        mysqlConnection = new ConnectionInfo(config.mysqlUrl, config.mysqlUsername, config.mysqlPassword, config.port);
        sqlTools = new SqlTools(mysqlConnection.createMysqlConnection());
        this.threadCount = config.maxThreadCount;
        for (int i = 0; i < threadCount; i++) {
            WorkThread workThread = new WorkThread(schemaMappingMap, mysqlConnection, sqlTools);
            threadList.add(workThread);
        }
    }

    private void initSchemaMappingMap(String schemaMappings) {
        String[] pairs = schemaMappings.split(";");
        for (String pair : pairs) {
            if (pair == null || " ".equals(pair)){
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
    }

    private void parseSinkRecordThread() {
        new Thread(this::parseRecord).start();
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
                e.printStackTrace();
            }
            assert sinkRecord != null;
            value = (Struct) sinkRecord.value();
            if (value == null) {
                continue;
            }
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

    private void findProperWorkThread(String tableFullName, SinkRecordObject sinkRecordObject, String schemaName){
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

    private int getCurrentCount() {
        int count = 0;
        for (WorkThread workThread : threadList) {
            count += workThread.count;
        }
        return count;
    }

    private int getRelyIndex(String tableFullName, String schemaName) {
        Set<String> set = runnableMap.keySet();
        Iterator<String> iterator = set.iterator();
        while (iterator.hasNext()) {
            String oldTableName = iterator.next();
            List<String> relyTableList = sqlTools.getRelyTableList(oldTableName, schemaName);
            if (relyTableList.size() == 0) {
                return -1;
            } else {
                for (String relyTable : relyTableList) {
                    if (relyTable.equals(tableFullName)) {
                        return runnableMap.get(oldTableName);
                    }
                }
            }
        }
        return -1;
    }

    private void statTask() {
        new Thread(() -> {
            int before = getCurrentCount();
            int delta = 0;
            while (true) {
                try {
                    Thread.sleep(1000);
                    delta = getCurrentCount() - before;
                    before = getCurrentCount();
                    String date = ofPattern.format(LocalDateTime.now());
                    String result = String.format("have replayed %s data, and current time is %s, and current " +
                            "speed is %s", getCurrentCount(), date, delta);
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info(result);
                    }
                } catch (InterruptedException exp) {
                    LOGGER.warn("Interrupted exception occurred", exp);
                }
            }
        }).start();
    }
}