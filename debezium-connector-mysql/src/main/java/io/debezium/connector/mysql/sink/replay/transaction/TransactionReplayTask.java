/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.sink.replay.transaction;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mysql.cj.jdbc.AbandonedConnectionCleanupThread;
import com.mysql.cj.util.StringUtils;

import io.debezium.config.Configuration;
import io.debezium.connector.breakpoint.BreakPointRecord;
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
    private static final long INVALID_VALUE = -1L;
    private static final int QUEUE_LIMIT = 100000;

    private int maxQueueSize;
    private double openFlowControlThreshold;
    private double closeFlowControlThreshold;

    private volatile AtomicBoolean isBlock = new AtomicBoolean(false);
    private ConnectionInfo openGaussConnection;
    private SqlTools sqlTools;
    private TransactionDispatcher transactionDispatcher;
    private MySqlSinkConnectorConfig config;
    private BreakPointRecord breakPointRecord;

    private int count = 0;
    private int queueIndex = 0;
    private long extractCount;
    private long skippedCount;
    private long skippedExcludeEventCount;
    private ArrayList<String> sqlList = new ArrayList<>();
    private LinkedList<Long> sqlOffsets = new LinkedList<>();
    private List<Long> toDeleteOffsets;
    private Transaction transaction = new Transaction();
    private String xlogLocation;
    private boolean isConnection = true;
    private Long sinkQueueFirstOffset;

    private HashMap<String, Long> dmlEventCountMap = new HashMap<String, Long>();
    private HashMap<String, String> tableSnapshotHashmap = new HashMap<>();
    private HashMap<String, TableMetaData> tableMetaDataMap = new HashMap<String, TableMetaData>();
    private HashMap<String, String> schemaMappingMap = new HashMap<>();

    private ArrayList<String> changedTableNameList = new ArrayList<>();
    private ArrayList<SinkRecordObject> sinkRecordsArrayList = new ArrayList<>();
    private BlockingQueue<String> feedBackQueue = new LinkedBlockingQueue<>();
    private BlockingQueue<SinkRecord> sinkQueue = new LinkedBlockingQueue<>();
    private ArrayList<ConcurrentLinkedQueue<Transaction>> transactionQueueList = new ArrayList<>();
    private Map<Long, String> addedQueueMap = new ConcurrentHashMap<>();

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
            LOGGER.error("Interrupt exception");
        }
    }

    private void writeXlogResult() {
        String xlogResult = sqlTools.getXlogLocation();
        sqlTools.closeConnection();
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(xlogLocation))) {
            bw.write("xlog.location=" + xlogResult);
        }
        catch (IOException exp) {
            LOGGER.error("Fail to write xlog location {}", xlogResult);
        }
        LOGGER.info("Online migration from mysql to openGauss has gracefully stopped and current xlog"
                + "location in openGauss is {}", xlogResult);
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

    /**
     * Init breakpoint record properties
     *
     * @param config mysql sink connector config
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

    private void initOpenGaussConnection(MySqlSinkConnectorConfig config) {
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
            transactionQueueList.add(new ConcurrentLinkedQueue<Transaction>());
        }
    }

    private void initSqlTools() {
        sqlTools = new SqlTools(openGaussConnection.createOpenGaussConnection());
    }

    private void initXlogLocation(String xlogLocation) {
        this.xlogLocation = xlogLocation;
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
        transactionDispatcher.initBreakPointRecord(breakPointRecord);
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
        int skipNum = 0;
        Struct value;
        SinkRecord sinkRecord = null;
        SourceField sourceField;
        String snapshot;
        SinkRecordObject sinkRecordObject;
        DataOperation dataOperation;
        while (isConnection) {
            try {
                sinkRecord = sinkQueue.take();
            }
            catch (InterruptedException e) {
                LOGGER.error("Interrupted exception occurred", e);
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
                        statExtractCount();
                        skippedExcludeEventCount++;
                        MysqlSinkProcessInfo.SINK_PROCESS_INFO.setSkippedExcludeEventCount(skippedExcludeEventCount);
                    }
                    if (skipNum == value.getInt64(TransactionRecordField.EVENT_COUNT)) {
                        addReplayedOffset(sinkQueueFirstOffset);
                        addReplayedOffset(sinkRecord.kafkaOffset());
                    }
                    transaction.setTxnEndOffset(sinkRecord.kafkaOffset());
                    try {
                        for (SinkRecordObject oneSinkRecordObject : sinkRecordsArrayList) {
                            constructDml(oneSinkRecordObject);
                        }
                        if (!isConnection) {
                            LOGGER.error("There is a connection problem with the openGauss,"
                                    + " check the database status or connection");
                            statExtractCount();
                            count++;
                            transactionDispatcher.addFailTransaction();
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
                                value.get(TransactionRecordField.ID),
                                value.getInt64(TransactionRecordField.EVENT_COUNT),
                                skipNum);
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
                    MysqlSinkProcessInfo.SINK_PROCESS_INFO.setSkippedCount(skippedCount);
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

    /**
     * Get the offset of already replayed record
     *
     * @return the continuous and maximum offset
     */
    @Override
    public Long getReplayedOffset() {
        PriorityBlockingQueue<Long> replayedOffsets = breakPointRecord.getReplayedOffset();
        Long offset = replayedOffsets.peek();
        Long endOffset = INVALID_VALUE;
        if (replayedOffsets.isEmpty()) {
            if (sinkQueueFirstOffset != null) {
                return sinkQueueFirstOffset;
            }
            else {
                return endOffset;
            }
        }
        boolean isContinuous = true;
        while (isContinuous && !replayedOffsets.isEmpty()) {
            Long num;
            try {
                num = replayedOffsets.take();
                if (num.equals(offset)) {
                    endOffset = num;
                }
                else {
                    replayedOffsets.offer(num);
                    isContinuous = false;
                }
                offset++;
            }
            catch (InterruptedException exp) {
                LOGGER.error("Interrupted exception occurred", exp);
            }
        }
        if (endOffset.equals(INVALID_VALUE)) {
            return endOffset;
        }
        replayedOffsets.offer(endOffset);
        Iterator<Map.Entry<Long, String>> iterator = addedQueueMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, String> entry = iterator.next();
            if (entry.getKey() < endOffset) {
                iterator.remove();
            }
        }
        if (addedQueueMap.size() > QUEUE_LIMIT || replayedOffsets.size() > QUEUE_LIMIT) {
            addedQueueMap.clear();
            replayedOffsets.clear();
            return INVALID_VALUE;
        }
        toDeleteOffsets.add(endOffset);
        return endOffset + 1;
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
        DdlOperation ddlOperation = null;
        if (sinkRecordObject.getDataOperation() instanceof DdlOperation) {
            ddlOperation = (DdlOperation) sinkRecordObject.getDataOperation();
        }
        else {
            return;
        }
        String schemaName = sourceField.getDatabase();
        String tableName = sourceField.getTable();
        String newSchemaName = schemaMappingMap.getOrDefault(schemaName, schemaName);
        String ddl = ddlOperation.getDdl();
        sqlList.add("set current_schema to " + newSchemaName + ";");
        if (StringUtils.isNullOrEmpty(tableName)) {
            sqlList.add(ddl);
        }
        else {
            String modifiedDdl = null;
            if (ddl.toLowerCase(Locale.ROOT).startsWith("alter table")
                    && ddl.toLowerCase(Locale.ROOT).contains("rename to")
                    && !ddl.toLowerCase(Locale.ROOT).contains("`rename to")) {
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
                modifiedDdl = ddl.replaceFirst(SqlTools.addingBackQuote(schemaName) + ".", "");
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
        transaction.setTxnBeginOffset(sinkRecordObject.getKafkaOffset());
        transaction.setTxnEndOffset(sinkRecordObject.getKafkaOffset());
        splitTransactionQueue();
        sqlList.clear();
    }

    private String ignoreSchemaName(String ddl, String schemaName, String tableName) {
        Set<String> schemaTableSet = new HashSet<>();
        schemaTableSet.add(schemaName + "." + tableName);
        schemaTableSet.add(SqlTools.addingBackQuote(schemaName) + "." + tableName);
        schemaTableSet.add(schemaName + "." + SqlTools.addingBackQuote(tableName));
        schemaTableSet.add(SqlTools.addingBackQuote(schemaName) + "." + SqlTools.addingBackQuote(tableName));
        for (String name : schemaTableSet) {
            if (ddl.contains(name)) {
                return ddl.replaceFirst(name, SqlTools.addingBackQuote(tableName));
            }
        }
        return ddl;
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
        statExtractCount();
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
            PreparedStatement ps = conn.prepareStatement("select v_schema_name, v_table_name, t_binlog_name,"
                    + " i_binlog_position from sch_chameleon.t_replica_tables;");
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                String schemaName = rs.getString("v_schema_name");
                String tableName = rs.getString("v_table_name");
                String binlogName = rs.getString("t_binlog_name");
                String binlogPosition = rs.getString("i_binlog_position");
                tableSnapshotHashmap.put(schemaMappingMap.getOrDefault(schemaName, schemaName) + "." + tableName,
                        binlogName + ":" + binlogPosition);
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
            String[] file = snapshotBinlogFile.split("\\.");
            String[] position = snapshotPoint.split(":");
            if (file.length >= 2 && position.length >= 2) {
                long snapshotFileIndex = Long.valueOf(file[1]);
                long snapshotBinlogPosition = Long.valueOf(position[1]);
                if (fileIndex < snapshotFileIndex
                        || (fileIndex == snapshotFileIndex && binlogPosition <= snapshotBinlogPosition)) {
                    String skipInfo = String.format("Table %s snapshot is %s, current position is %s, which is less than "
                            + "table snapshot, so skip the record.", fullName, snapshotPoint,
                            binlogFile + ":" + binlogPosition);
                    LOGGER.warn(skipInfo);
                    return true;
                }
            }
        }
        return false;
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

    private void statExtractCount() {
        extractCount++;
        MysqlSinkProcessInfo.SINK_PROCESS_INFO.setExtractCount(extractCount);
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
}
