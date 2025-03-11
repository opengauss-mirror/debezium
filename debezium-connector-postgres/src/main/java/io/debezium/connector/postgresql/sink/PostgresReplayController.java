/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package io.debezium.connector.postgresql.sink;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import io.debezium.connector.postgresql.process.PgOfflineSinkProcessInfo;
import io.debezium.connector.postgresql.process.PgProcessCommitter;
import io.debezium.connector.postgresql.process.PgSinkProcessInfo;
import io.debezium.connector.postgresql.process.ProgressInfo;
import io.debezium.connector.postgresql.process.ProgressStatus;
import io.debezium.connector.postgresql.process.TotalInfo;
import io.debezium.connector.postgresql.sink.object.ConnectionInfo;
import io.debezium.connector.postgresql.sink.object.DataReplayOperation;
import io.debezium.connector.postgresql.sink.object.TableMetaData;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.postgresql.replication.LogSequenceNumber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.postgresql.migration.PostgresSqlConstant;
import io.debezium.connector.postgresql.sink.common.SourceDataField;
import io.debezium.connector.postgresql.sink.record.SinkDataRecord;
import io.debezium.connector.postgresql.sink.record.SinkMetadataRecord;
import io.debezium.connector.postgresql.sink.record.SinkObjectRecord;
import io.debezium.connector.postgresql.sink.task.PostgresSinkConnectorConfig;
import io.debezium.connector.postgresql.sink.utils.SqlTools;
import io.debezium.connector.postgresql.sink.utils.TimeUtil;
import io.debezium.connector.postgresql.sink.worker.PostgresDataReplayWorkThread;
import io.debezium.connector.postgresql.sink.worker.PostgresMetadataReplayWorkThread;
import io.debezium.connector.postgresql.sink.worker.PostgresObjectReplayWorkThread;
import io.debezium.data.Envelope;
import io.debezium.migration.BaseMigrationConfig;
import io.debezium.migration.ObjectEnum;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.sink.ReplayController;
import io.debezium.sink.worker.ReplayWorkThread;

/**
 * Description: JdbcDbWriter
 *
 * @author tianbin
 * @since 2024-11-20
 */
public class PostgresReplayController implements ReplayController {
    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresReplayController.class);
    private static final String INSERT = "c";
    private static final String UPDATE = "u";
    private static final String DELETE = "d";
    private static final String TRUNCATE = "t";
    private static final String PATH = "p";
    private static final String FIELD_TABLE = "table";
    private static final String INDEX = "i";
    private static final String SNAPSHOT = "ts";
    private static final String MSGTYPE = "msgType";
    private static final int TASK_GRACEFUL_SHUTDOWN_TIME = 5;
    private static final int TIME_UNIT = 1000;
    private static final int MEMORY_UNIT = 1024;
    private static volatile Boolean isParseMetaFinished = false;

    private final ThreadPoolExecutor threadPool = new ThreadPoolExecutor(4, 4, 100,
            TimeUnit.SECONDS, new LinkedBlockingQueue<>(4));
    private final ScheduledExecutorService fullProgressReportService = Executors
            .newSingleThreadScheduledExecutor((r) -> new Thread(r, "fullProgressReportThread"));
    private final ScheduledExecutorService objectProgressReportService = Executors
            .newSingleThreadScheduledExecutor((r) -> new Thread(r, "objectProgressReportThread"));
    private final DateTimeFormatter ofPattern = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    private final Map<String, String> tableSnapshotHashmap = new HashMap<>();
    private final Map<String, TableMetaData> oldTableMap = new HashMap<>();

    private Map<ObjectEnum, List<ProgressInfo>> objectProgressMap;
    private int threadCount;
    private int runCount = 0;
    private ConnectionInfo databaseConnection;
    private SqlTools sqlTools;
    private PgProcessCommitter failSqlCommitter;
    private PostgresSinkConnectorConfig config;
    private ArrayList<PostgresDataReplayWorkThread> threadList = new ArrayList<>();
    private PostgresObjectReplayWorkThread objectThread;
    private PostgresMetadataReplayWorkThread metaDataThread;
    private List<Long> toDeleteOffsets;
    private BlockingQueue<SinkRecord> sinkQueue = new LinkedBlockingQueue<>();
    private Map<String, Integer> runnableMap = new HashMap<>();
    private Map<String, String> schemaMappingMap = new HashMap<>();
    private volatile AtomicBoolean isSinkQueueBlock = new AtomicBoolean(false);
    private volatile AtomicBoolean isWorkQueueBlock = new AtomicBoolean(false);
    private volatile AtomicBoolean isConnectionAlive = new AtomicBoolean(true);
    private int maxQueueSize;
    private double openFlowControlThreshold;
    private double closeFlowControlThreshold;
    private boolean isStop = false;
    private PgProcessCommitter pgSinkFullCommiter = null;
    private PgProcessCommitter pgSinkObjectCommiter = null;
    private List<String> tableList;
    private boolean isBpCondition = false;
    private int filterCount = 0;
    private int sqlErrCount;
    private BaseMigrationConfig.MessageType msgStatus = null;
    private volatile BaseMigrationConfig.MigrationType migrationType = null;
    private Map<String, ProgressInfo> progressMap;
    private boolean isInitFullProcess = false;
    private boolean isInitObjectProcess = false;
    private long startTime;

    /**
     * Constructor
     *
     * @param config OpengaussSinkConnectorConfig the config
     */
    public PostgresReplayController(PostgresSinkConnectorConfig config) {
        this.config = config;
        initSchemaMappingMap(config.schemaMappings);
        databaseConnection = new ConnectionInfo(config, isConnectionAlive);
        sqlTools = new SqlTools(databaseConnection.createOpenGaussConnection());

        this.threadCount = config.maxThreadCount;
        progressMap = new HashMap<>();
        for (int i = 0; i < threadCount; i++) {
            PostgresDataReplayWorkThread workThread = new PostgresDataReplayWorkThread(schemaMappingMap,
                    databaseConnection, sqlTools, i, config.isCommitProcess(), progressMap);
            workThread.setClearFile(config.isDelCsv);
            threadList.add(workThread);
        }
        // use for parse object data
        objectProgressMap = new HashMap<>();
        objectThread = new PostgresObjectReplayWorkThread(schemaMappingMap,
                databaseConnection, config.isCommitProcess(), objectProgressMap);
        // use for parse meta data
        metaDataThread = new PostgresMetadataReplayWorkThread(schemaMappingMap,
                databaseConnection, config.createTableWithOptions);
        this.failSqlCommitter = new PgProcessCommitter(config.getFailSqlPath(), config.getFileSizeLimit());
        initFlowControl();
        initSnapshotRecordTable();
        printSinkRecordObject();
    }

    private void initSnapshotRecordTable() {
        try (Connection connection = databaseConnection.createOpenGaussConnection();
             Statement statement = connection.createStatement();) {
            for (String sql : PostgresSqlConstant.SNAPSHOT_TABLE_CREATE_SQL) {
                statement.addBatch(sql);
            }
            statement.executeBatch();
            LOGGER.info("create snapshot record table successfully");
        } catch (SQLException e) {
            LOGGER.error("create snapshot record table occurred error: ", e);
        }
    }

    /**
     * Do stop
     */
    public void doStop() {
        isStop = true;
        for (ReplayWorkThread workThread : threadList) {
            workThread.setIsStop(true);
        }
        try {
            TimeUnit.SECONDS.sleep(TASK_GRACEFUL_SHUTDOWN_TIME - 1);
            writeXlogResult();
            closeConnection();
        } catch (InterruptedException exp) {
            LOGGER.error("Interrupt exception");
        }
    }

    private void writeXlogResult() {
        if (!isMigrationIncremental()) {
            return;
        }
        String xlogResult = sqlTools.getXlogLocation();
        sqlTools.closeConnection();
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(config.xlogLocation))) {
            bw.write("xlog.location=" + xlogResult);
        } catch (IOException exp) {
            LOGGER.error("Fail to write xlog location {}", xlogResult, exp);
        }
        LOGGER.info("Online migration from postgresql to openGauss has gracefully stopped and current xlog"
                + "location in openGauss is {}", xlogResult);
    }

    private void closeConnection() {
        for (ReplayWorkThread workThread : threadList) {
            if (workThread.getConnection() != null) {
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
    }

    private void initSchemaMappingMap(String schemaMappings) {
        String[] pairs = schemaMappings.split(";");
        for (String pair : pairs) {
            if (pair == null || pair.trim().isEmpty()) {
                LOGGER.error("the format of schema.mappings is error:" + schemaMappings);
                continue;
            }
            String[] schema = pair.split(":");
            if (schema.length == 2) {
                schemaMappingMap.put(schema[0].trim(), schema[1].trim());
            } else {
                LOGGER.error("schemaMapping configuration invalid, {}", schema);
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
        getTableSnapshot();
        parseSinkRecordThread();
        statTask();
        if (config.isCommitProcess()) {
            statCommit();
            startTime = System.currentTimeMillis();
            fullProgressReportService.scheduleAtFixedRate(this::fullProgressReport,
                    config.getCommitTimeInterval(), config.getCommitTimeInterval(), TimeUnit.SECONDS);
            objectProgressReportService.scheduleAtFixedRate(this::objectProgressReport,
                    config.getCommitTimeInterval(), config.getCommitTimeInterval(), TimeUnit.SECONDS);
        }
        statReplayTask();
    }

    private void getTableSnapshot() {
        try (Connection conn = databaseConnection.createOpenGaussConnection();
             PreparedStatement ps = conn.prepareStatement(PostgresSqlConstant.SELECT_SNAPSHOT_RECORD_SQL);
             ResultSet rs = ps.executeQuery();) {
            while (rs.next()) {
                String schemaName = rs.getString("pg_schema_name");
                String tableName = rs.getString("pg_table_name");
                String location = rs.getString("pg_xlog_location");
                tableSnapshotHashmap.put(getSinkSchema(schemaName) + "." + tableName, location);
            }
            LOGGER.info("load table snapshot record finished.");
        } catch (SQLException exp) {
            LOGGER.warn("An error occurred while getting table snapshot from sch_debezium.pg_replica_tables.", exp);
        }
    }

    /**
     * Gets sink database schema according to schema mapping
     *
     * @param sourceSchema String the source database schema name
     * @return String the sink database schema name
     */
    protected String getSinkSchema(String sourceSchema) {
        return schemaMappingMap.getOrDefault(sourceSchema, sourceSchema);
    }

    private void waitMigrationType(Thread thread) {
        int seconds = 0;
        while (migrationType == null) {
            TimeUtil.sleep(1000);
            if ((seconds++ % 5) == 0) {
                LOGGER.info("{} thread wait determinte migration type", thread.getName());
            }
        }
    }

    private void parseSinkRecordThread() {
        threadPool.execute(this::parseRecord);
    }

    /**
     * parse record
     */
    public void parseRecord() {
        LOGGER.info("parse data thread start");
        SinkRecord sinkRecord = null;
        Struct value = null;
        while (!isStop) {
            try {
                sinkRecord = sinkQueue.take();
            } catch (InterruptedException e) {
                LOGGER.error("Interrupted exception occurred", e);
            }
            if (sinkRecord == null) {
                continue;
            }
            if (sinkRecord.value() instanceof Struct) {
                value = (Struct) sinkRecord.value();
            } else {
                value = null;
            }
            if (value == null) {
                // sink record of delete will bring a null record,the record offset add to sqlKafkaOffsets
                continue;
            }
            distributeRecord(sinkRecord, value);
        }
        LOGGER.info("main thread exiting, migration finished.");
    }

    private void stopAllWorkThread() {
        for (PostgresDataReplayWorkThread thread : threadList) {
            thread.setShouldStop(true);
        }
        metaDataThread.setShouldStop(true);
        objectThread.setShouldStop(true);

        boolean isAllThreadExit = false;
        while (!isAllThreadExit) {
            isAllThreadExit = isAllThreadsExited();
            TimeUtil.sleep(1000);
        }
        LOGGER.info("all work thread have exited");
    }

    private boolean isAllThreadsExited() {
        for (PostgresDataReplayWorkThread thread : threadList) {
            if (thread.isAlive()) {
                return false;
            }
        }
        return !metaDataThread.isAlive() && !objectThread.isAlive();
    }

    private void waitMetaThreadFinish() {
        metaDataThread.setShouldStop(true);
        while (metaDataThread.isAlive()) {
            TimeUtil.sleep(500);
        }
    }

    private boolean isDataType(String msgType) {
        return BaseMigrationConfig.MessageType.INCREMENTALDATA.toString().equals(msgType)
                || BaseMigrationConfig.MessageType.FULLDATA.toString().equals(msgType);
    }

    private void distributeRecord(SinkRecord sinkRecord, Struct value) {
        String msgType = value.getString(Envelope.FieldName.MSGTYPE);
        if (isDataType(msgType)) {
            // status: metadata -> data
            if (BaseMigrationConfig.MessageType.METADATA.equals(getMsgStatus())) {
                waitMetaThreadFinish();
            }
            setMsgStatus(BaseMigrationConfig.MessageType.valueOf(msgType));
            if (migrationType == null) {
                setMigrationType(BaseMigrationConfig.MigrationType.INCREMENTAL);
            }
            distributeDataRecord(sinkRecord, value);
        } else if (BaseMigrationConfig.MessageType.OBJECT.toString().equals(msgType)) {
            // null -> object
            setMsgStatus(BaseMigrationConfig.MessageType.OBJECT);
            if (migrationType == null) {
                setMigrationType(BaseMigrationConfig.MigrationType.OBJECT);
            }
            distributeObjectRecord(value);
        } else if (BaseMigrationConfig.MessageType.METADATA.toString().equals(msgType)) {
            // null -> metadata
            setMsgStatus(BaseMigrationConfig.MessageType.METADATA);
            if (migrationType == null) {
                setMigrationType(BaseMigrationConfig.MigrationType.FULL);
            }
            distributeMetaRecord(value);
        } else if (BaseMigrationConfig.MessageType.EOF.toString().equals(msgType)) {
            stopAllWorkThread();
            isStop = true;
        } else {
            LOGGER.warn("unknown msgType: {}", msgType);
        }
    }

    private void distributeMetaRecord(Struct value) {
        String schema = value.getString(HistoryRecord.Fields.SCHEMA_NAME);
        Struct source = value.getStruct(HistoryRecord.Fields.SOURCE);
        String table = source.getString(FIELD_TABLE);
        SinkMetadataRecord sinkMetadataRecord = new SinkMetadataRecord(schema, table,
                value.getArray(HistoryRecord.Fields.TABLE_CHANGES), value.getString(HistoryRecord.Fields.PARENT_TABLES),
                value.getString(HistoryRecord.Fields.PARTITION_INFO));
        metaDataThread.addData(sinkMetadataRecord);
        if (!metaDataThread.isRunning()) {
            metaDataThread.start();
            metaDataThread.setRunning(true);
        }
    }

    private void distributeObjectRecord(Struct value) {
        SinkObjectRecord sinkObjectRecord = new SinkObjectRecord();
        sinkObjectRecord.setDdl(value.getString(HistoryRecord.Fields.DDL_STATEMENTS));
        sinkObjectRecord.setObjType(ObjectEnum.valueOf(value.getString(HistoryRecord.Fields.OBJECT_TYPE)));
        sinkObjectRecord.setObjName(value.getString(HistoryRecord.Fields.OBJECT_NAME));
        sinkObjectRecord.setSchema(value.getString(HistoryRecord.Fields.SCHEMA_NAME));
        objectThread.addData(sinkObjectRecord);
        if (!objectThread.isRunning()) {
            objectThread.start();
            objectThread.setRunning(true);
        }
    }

    private void distributeDataRecord(SinkRecord sinkRecord, Struct value) {
        DataReplayOperation dataReplayOperation = new DataReplayOperation(value);
        SourceDataField sourceDataField = new SourceDataField(value);
        if (isIncrementalData(value) && isSkippedEvent(sourceDataField)) {
            PgSinkProcessInfo.SINK_PROCESS_INFO.autoIncreaseSkippedCount();
            LOGGER.warn("Skip one record: {}", sourceDataField);
            return;
        }
        Long kafkaOffset = sinkRecord.kafkaOffset();
        SinkDataRecord sinkDataRecord = new SinkDataRecord();
        sinkDataRecord.setDmlOperation(dataReplayOperation);
        sinkDataRecord.setSourceField(sourceDataField);
        sinkDataRecord.setKafkaOffset(kafkaOffset);
        sinkDataRecord.setMsgType(value.getString(MSGTYPE));
        String schemaName = sourceDataField.getSchema();
        while (isWorkQueueBlock()) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException exp) {
                LOGGER.warn("Receive interrupted exception while work queue block:{}", exp.getMessage());
            }
        }
        String tableName = sourceDataField.getTable();
        String tableFullName = schemaMappingMap.get(schemaName) + "." + tableName;
        findProperWorkThread(tableFullName, sinkDataRecord);
    }

    private boolean isIncrementalData(Struct value) {
        return BaseMigrationConfig.MessageType.INCREMENTALDATA.code().equals(value.getString(MSGTYPE));
    }

    private boolean isSkippedEvent(SourceDataField sourceDataField) {
        String schemaName = sourceDataField.getSchema();
        String tableName = sourceDataField.getTable();
        String fullName = getSinkSchema(schemaName) + "." + tableName;
        if (tableSnapshotHashmap.containsKey(fullName)) {
            long lsn = sourceDataField.getLsn();
            String snapshotPoint = tableSnapshotHashmap.get(fullName);
            if (lsn <= LogSequenceNumber.valueOf(snapshotPoint).asLong()) {
                LOGGER.warn("Table {} snapshot is {}, current position is {}, "
                                + "which is less than table snapshot, so skip the record.", fullName,
                        snapshotPoint, lsn);
                return true;
            }
        }
        return false;
    }

    private void findProperWorkThread(String tableFullName, SinkDataRecord sinkDataRecord) {
        if (runnableMap.containsKey(tableFullName)) {
            PostgresDataReplayWorkThread workThread = threadList.get(runnableMap.get(tableFullName));
            workThread.addData(sinkDataRecord);
            return;
        }
        PostgresDataReplayWorkThread workThread;
        if (runCount < threadCount) {
            workThread = threadList.get(runCount);
            workThread.addData(sinkDataRecord);
            workThread.start();
        } else {
            workThread = threadList.get(runCount % threadCount);
            workThread.addData(sinkDataRecord);
        }
        runnableMap.put(tableFullName, runCount % threadCount);
        runCount++;
    }

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
                SinkDataRecord sinkDataRecord = null;
                String workThreadName = "";
                for (PostgresDataReplayWorkThread workThread : threadList) {
                    SinkDataRecord record = workThread.getThreadSinkRecordObject();
                    if (record != null) {
                        if (sinkDataRecord == null || record.getSourceField()
                                .getLsn() < sinkDataRecord.getSourceField().getLsn()) {
                            sinkDataRecord = record;
                            workThreadName = workThread.getName();
                        }
                    }
                }
                if (sinkDataRecord != null) {
                    LOGGER.warn("[Breakpoint] {} in work thread {}",
                            sinkDataRecord.getSourceField().toString(), workThreadName);
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
                LOGGER.warn("[start flow control sink queue] current isSinkQueueBlock is {}, queue size is {}, "
                                + "which is more than {} * {}, so open flow control",
                        isSinkQueueBlock, size, openFlowControlThreshold, maxQueueSize);
                isSinkQueueBlock.set(true);
            }
        }
        if (size < closeFlowControlQueueSize) {
            if (isSinkQueueBlock.get()) {
                LOGGER.warn("[close flow control sink queue] current isSinkQueueBlock is {}, queue size is {}, "
                                + "which is less than {} * {}, so close flow control",
                        isSinkQueueBlock, size, closeFlowControlThreshold, maxQueueSize);
                isSinkQueueBlock.set(false);
            }
        }
    }

    private void getWorkThreadQueueFlag() {
        int openFlowControlQueueSize = (int) (openFlowControlThreshold * maxQueueSize);
        int closeFlowControlQueueSize = (int) (closeFlowControlThreshold * maxQueueSize);
        int size = 0;
        for (PostgresDataReplayWorkThread workThread : threadList) {
            size = workThread.getQueueLength();
            if (size > openFlowControlQueueSize) {
                if (!isWorkQueueBlock.get()) {
                    LOGGER.warn("[start flow control work queue] current isWorkQueueBlock is {}, "
                            + "queue size is {}, which is more than {} * {}, so open flow control",
                            isWorkQueueBlock, size, openFlowControlThreshold, maxQueueSize);
                    isWorkQueueBlock.set(true);
                    return;
                }
            }
            if (size < closeFlowControlQueueSize) {
                workThread.setFreeBlock(true);
            } else {
                workThread.setFreeBlock(false);
            }
        }
        if (isFreeBlock(threadList) && isWorkQueueBlock()) {
            LOGGER.warn("[close flow control work queue] current isWorkQueueBlock is {}, all the queue size is "
                    + "less than {} * {}, so close flow control",
                    isWorkQueueBlock, closeFlowControlThreshold, maxQueueSize);
            isWorkQueueBlock.set(false);
        }
    }

    private boolean isFreeBlock(ArrayList<PostgresDataReplayWorkThread> threadList) {
        for (PostgresDataReplayWorkThread workThread : threadList) {
            if (!workThread.isFreeBlock()) {
                return false;
            }
        }
        return true;
    }

    private void initFlowControl() {
        this.maxQueueSize = config.maxQueueSize;
        this.openFlowControlThreshold = config.openFlowControlThreshold;
        this.closeFlowControlThreshold = config.closeFlowControlThreshold;
        monitorSinkQueueSize();
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
     * Is block
     *
     * @return boolean true if is block
     */
    public boolean isSinkQueueBlock() {
        return this.isSinkQueueBlock.get();
    }

    /**
     * Is database connection alive
     *
     * @return true if database connection is alive
     */
    public boolean isConnectionAlive() {
        return isConnectionAlive.get();
    }

    private int[] getSuccessAndFailCount() {
        int successCount = 0;
        int failCount = sqlErrCount;
        for (PostgresDataReplayWorkThread workThread : threadList) {
            successCount += workThread.getSuccessCount();
            failCount += workThread.getFailCount();
        }
        return new int[]{successCount, failCount, successCount + failCount};
    }

    private int getRelyIndex(String currentTableName) {
        Set<String> set = runnableMap.keySet();
        for (String previousTableName : set) {
            if (sqlTools.getForeignTableList(previousTableName).contains(currentTableName)
                    || sqlTools.getForeignTableList(currentTableName).contains(previousTableName)) {
                return runnableMap.get(previousTableName);
            }
        }
        return -1;
    }

    private void statTask() {
        threadPool.execute(() -> {
            waitMigrationType(Thread.currentThread());
            if (!isMigrationIncremental()) {
                LOGGER.info("not incremental, statTask thread exiting");
                return;
            }
            LOGGER.info("incremental statTask thread start");
            int before = getSuccessAndFailCount()[2];
            while (true) {
                try {
                    Thread.sleep(1000);
                    if (LOGGER.isInfoEnabled()) {
                        // Incremental migration progress logs
                        LOGGER.info("incremental migration have replayed {} data, and current time is {}, "
                                + "and current speed is {}", getSuccessAndFailCount()[2],
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
            waitMigrationType(Thread.currentThread());
            if (!isMigrationIncremental()) {
                LOGGER.info("not incremental, statCommit thread exiting");
                return;
            }
            LOGGER.info("statCommit thread start");
            PgProcessCommitter processCommitter = new PgProcessCommitter(config);
            processCommitter.commitSinkProcessInfo();
        });
    }

    private void statReplayTask() {
        threadPool.execute(() -> {
            waitMigrationType(Thread.currentThread());
            if (!isMigrationIncremental()) {
                LOGGER.info("not incremental, statReplayTask thread exiting");
                return;
            }
            LOGGER.info("statReplayTask thread start");
            while (true) {
                PgSinkProcessInfo.SINK_PROCESS_INFO.setSuccessCount(getSuccessAndFailCount()[0]);
                PgSinkProcessInfo.SINK_PROCESS_INFO.setFailCount(getSuccessAndFailCount()[1]);
                PgSinkProcessInfo.SINK_PROCESS_INFO.setReplayedCount(getSuccessAndFailCount()[2]);
                List<String> failSqlList = collectFailSql();
                if (failSqlList.size() > 0) {
                    commitFailSql(failSqlList);
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    LOGGER.error("Interrupted exception occurred while thread sleeping", e);
                }
            }
        });
    }

    private List<String> collectFailSql() {
        List<String> failSqlList = new ArrayList<>();
        for (PostgresDataReplayWorkThread workThread : threadList) {
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

    private void objectProgressReport() {
        if (migrationType == null) {
            LOGGER.info("object progress report wait determinte migration type");
            return;
        }
        if (!isMigrationObject()) {
            objectProgressReportService.shutdown();
            LOGGER.info("not object migration, objectProgressReportService shutdown");
            return;
        }
        if (!isInitObjectProcess) {
            initObjectProcess();
        }
        PgOfflineSinkProcessInfo pgObjectSinkProcessInfo = new PgOfflineSinkProcessInfo();
        for (Map.Entry<ObjectEnum, List<ProgressInfo>> objectProgerssEntry : objectProgressMap.entrySet()) {
            ObjectEnum objType = objectProgerssEntry.getKey();
            List<ProgressInfo> progressInfoList = objectProgerssEntry.getValue();
            progressInfoList.forEach(progressInfo -> {
                pgObjectSinkProcessInfo.addObject(progressInfo, objType);
            });
        }
        writeObjectToFile(pgObjectSinkProcessInfo);
        if (isStop) {
            objectProgressReportService.shutdown();
            LOGGER.info("object migration complete. object progress report thread is close.");
        }
    }

    private void fullProgressReport() {
        if (migrationType == null) {
            LOGGER.info("full progress report wait determine migration type");
            return;
        }
        if (!isMigrationFull()) {
            fullProgressReportService.shutdown();
            LOGGER.info("not full migration, fullProgressReportService shutdown");
            return;
        }
        if (!isInitFullProcess) {
            initFullProcess();
        }
        PgOfflineSinkProcessInfo pgFullSinkProcessInfo = new PgOfflineSinkProcessInfo();
        long totalRecord = 0L;
        double totalData = 0;
        for (String tableName : tableList) {
            ProgressInfo progressInfo = progressMap.get(tableName);
            if (progressInfo == null) {
                progressInfo = new ProgressInfo();
                progressInfo.setName(tableName);
                progressInfo.setData(0);
                progressInfo.setError("");
                if (isStop) {
                    progressInfo.setStatus(ProgressStatus.MIGRATED_COMPLETE.getCode());
                    progressInfo.setPercent(1);
                } else {
                    progressInfo.setStatus(ProgressStatus.NOT_MIGRATED.getCode());
                    progressInfo.setPercent(0);
                }
            }
            pgFullSinkProcessInfo.addTable(progressInfo);
            totalData += progressInfo.getData();
            totalRecord += progressInfo.getRecord();
        }
        long currentTime = System.currentTimeMillis();
        int timeInterval = (int) ((currentTime - startTime) / TIME_UNIT);
        BigDecimal speed = new BigDecimal(totalData / (timeInterval * MEMORY_UNIT * MEMORY_UNIT));
        pgFullSinkProcessInfo.setTotal(new TotalInfo(totalRecord,
                getFormatDouble(new BigDecimal(totalData / (MEMORY_UNIT * MEMORY_UNIT)), 2),
                timeInterval, getFormatDouble(speed, 2)));
        writeFullToFile(pgFullSinkProcessInfo);
        if (isStop) {
            fullProgressReportService.shutdown();
            LOGGER.info("full data migration complete. full report thread is close.");
        }
    }

    private double getFormatDouble(BigDecimal decimal, int precision) {
        return decimal.setScale(precision, RoundingMode.HALF_UP).doubleValue();
    }

    private void initFullProcess() {
        if (pgSinkFullCommiter == null) {
            pgSinkFullCommiter = new PgProcessCommitter(config,
                    migrationType.code() + PgProcessCommitter.PROCESS_SUFFIX);
        }
        if (pgSinkFullCommiter != null && pgSinkFullCommiter.hasMessage()) {
            tableList = pgSinkFullCommiter.getSourceTableList();
            isInitFullProcess = true;
            LOGGER.info("init full progress finished, total {} tables", tableList.size());
        }
    }

    private void initObjectProcess() {
        if (pgSinkObjectCommiter == null) {
            pgSinkObjectCommiter = new PgProcessCommitter(config,
                    migrationType.code() + PgProcessCommitter.PROCESS_SUFFIX);
        }
        isInitObjectProcess = true;
    }

    private void writeFullToFile(PgOfflineSinkProcessInfo pgFullSinkProcessInfo) {
        pgSinkFullCommiter.commitSinkTableProcessInfo(pgFullSinkProcessInfo);
    }

    private void writeObjectToFile(PgOfflineSinkProcessInfo pgObjectSinkProcessInfo) {
        pgSinkObjectCommiter.commitSinkTableProcessInfo(pgObjectSinkProcessInfo);
    }

    /**
     * Get connection status
     *
     * @return AtomicBoolean the connection status
     */
    public AtomicBoolean getConnectionStatus() {
        return isConnectionAlive;
    }

    public BaseMigrationConfig.MessageType getMsgStatus() {
        return msgStatus;
    }

    public void setMsgStatus(BaseMigrationConfig.MessageType msgStatus) {
        this.msgStatus = msgStatus;
    }

    public void setMigrationType(BaseMigrationConfig.MigrationType migrationType) {
        this.migrationType = migrationType;
    }

    public BaseMigrationConfig.MigrationType getMigrationType() {
        return migrationType;
    }

    private boolean isMigrationIncremental() {
        return BaseMigrationConfig.MigrationType.INCREMENTAL.equals(migrationType);
    }

    private boolean isMigrationFull() {
        return BaseMigrationConfig.MigrationType.FULL.equals(migrationType);
    }

    private boolean isMigrationObject() {
        return BaseMigrationConfig.MigrationType.OBJECT.equals(migrationType);
    }
}
