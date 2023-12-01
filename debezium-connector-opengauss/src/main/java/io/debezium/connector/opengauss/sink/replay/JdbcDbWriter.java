/**
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.opengauss.sink.replay;

import io.debezium.connector.breakpoint.BreakPointRecord;
import io.debezium.connector.opengauss.process.OgFullSinkProcessInfo;
import io.debezium.connector.opengauss.process.OgFullSourceProcessInfo;
import io.debezium.connector.opengauss.process.OgProcessCommitter;
import io.debezium.connector.opengauss.process.OgSinkProcessInfo;
import io.debezium.connector.opengauss.process.ProgressStatus;
import io.debezium.connector.opengauss.process.TableInfo;
import io.debezium.connector.opengauss.process.TotalInfo;
import io.debezium.connector.opengauss.sink.object.ConnectionInfo;
import io.debezium.connector.opengauss.sink.object.DmlOperation;
import io.debezium.connector.opengauss.sink.object.SinkRecordObject;
import io.debezium.connector.opengauss.sink.object.SourceField;
import io.debezium.connector.opengauss.sink.object.TableMetaData;
import io.debezium.connector.opengauss.sink.task.OpengaussSinkConnectorConfig;
import io.debezium.connector.opengauss.sink.utils.MysqlSqlTools;
import io.debezium.connector.opengauss.sink.utils.OpengaussSqlTools;
import io.debezium.connector.opengauss.sink.utils.OracleSqlTools;
import io.debezium.connector.opengauss.sink.utils.SqlTools;
import io.debezium.data.Envelope;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Description: JdbcDbWriter
 *
 * @author wangzhengyuan
 * @since 2022-11-20
 */
public class JdbcDbWriter {
    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcDbWriter.class);

    private static final String INSERT = "c";
    private static final String UPDATE = "u";
    private static final String DELETE = "d";
    private static final int TASK_GRACEFUL_SHUTDOWN_TIME = 5;
    private static final int BREAKPOINT_REPEAT_COUNT_LIMIT = 3000;

    private int threadCount;
    private int runCount;
    private ConnectionInfo databaseConnection;
    private SqlTools sqlTools;
    private OgProcessCommitter failSqlCommitter;
    private OpengaussSinkConnectorConfig config;
    private BreakPointRecord breakPointRecord;
    private ArrayList<WorkThread> threadList = new ArrayList<>();

    private List<Long> toDeleteOffsets;
    private final ThreadPoolExecutor threadPool = new ThreadPoolExecutor(4, 4, 100,
            TimeUnit.SECONDS, new LinkedBlockingQueue<>(4));
    private final ScheduledExecutorService fullProgressReportService = Executors
            .newSingleThreadScheduledExecutor((r) -> {
                return new Thread(r, "fullProgressReportThread");
            });
    private final DateTimeFormatter ofPattern = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    private BlockingQueue<SinkRecord> sinkQueue = new LinkedBlockingQueue<>();
    private Map<String, Integer> runnableMap = new HashMap<>();
    private Map<String, String> schemaMappingMap = new HashMap<>();
    private final Map<String, TableMetaData> oldTableMap = new HashMap<>();
    private Map<Long, Long> addedQueueMap = new ConcurrentHashMap<>();
    private volatile AtomicBoolean isSinkQueueBlock = new AtomicBoolean(false);
    private volatile AtomicBoolean isWorkQueueBlock = new AtomicBoolean(false);

    private int maxQueueSize;
    private double openFlowControlThreshold;
    private double closeFlowControlThreshold;
    private boolean isStop = false;
    private OgFullSinkProcessInfo ogFullSinkProcessInfo;
    private OgProcessCommitter ogSinkFullCommiter;
    private List<TableInfo> tableList;
    private boolean isBpCondition = false;
    private int filterCount = 0;
    private int sqlErrCount;

    /**
     * Constructor
     *
     * @param config OpengaussSinkConnectorConfig the config
     */
    public JdbcDbWriter(OpengaussSinkConnectorConfig config) {
        this.config = config;
        initSchemaMappingMap(config.schemaMappings);
        initRecordBreakpoint(config);
        databaseConnection = new ConnectionInfo(config.databaseUrl, config.databaseUsername, config.databasePassword, config.port, config.databaseType);
        if ("mysql".equals(config.databaseType.toLowerCase(Locale.ROOT))) {
            sqlTools = new MysqlSqlTools(databaseConnection.createMysqlConnection());
        } else if ("oracle".equals(config.databaseType.toLowerCase(Locale.ROOT))) {
            databaseConnection.setDatabase(config.database);
            sqlTools = new OracleSqlTools(databaseConnection.createOracleConnection());
        } else {
            sqlTools = new OpengaussSqlTools(databaseConnection.createOpenGaussConnection());
        }
        this.threadCount = config.maxThreadCount;
        for (int i = 0; i < threadCount; i++) {
            WorkThread workThread = new WorkThread(schemaMappingMap, databaseConnection, sqlTools, i, breakPointRecord);
            workThread.setClearFile(config.isDelCsv);
            threadList.add(workThread);
        }
        this.failSqlCommitter = new OgProcessCommitter(config.getFailSqlPath(), config.getFileSizeLimit());
        initFlowControl(config);
        printSinkRecordObject();
    }

    /**
     * Do stop
     */
    public void doStop() {
        isStop = true;
        for (WorkThread workThread : threadList) {
            workThread.setIsStop(true);
        }
        try {
            TimeUnit.SECONDS.sleep(TASK_GRACEFUL_SHUTDOWN_TIME - 1);
            closeConnection();
        } catch (InterruptedException exp) {
            LOGGER.error("Interrupt exception");
        }
    }

    private void closeConnection() {
        for (WorkThread workThread : threadList) {
            if (workThread.getConnection() != null){
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
     * Init breakpoint record properties
     *
     * @param config OpengaussSinkConnectorConfig openGauss sink connector config
     */
    private void initRecordBreakpoint(OpengaussSinkConnectorConfig config) {
        breakPointRecord = new BreakPointRecord(config);
        toDeleteOffsets = breakPointRecord.getToDeleteOffsets();
        breakPointRecord.start();
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
     * create work thread
     */
    public void createWorkThread() {
        parseSinkRecordThread();
        statTask();
        if (config.isCommitProcess()) {
            statCommit();
            ogSinkFullCommiter = new OgProcessCommitter(config, OgProcessCommitter.REVERSE_FULL_PROCESS_SUFFIX);
            fullProgressReportService.scheduleAtFixedRate(this::fullProgressReport,
                    config.getCommitTimeInterval(), config.getCommitTimeInterval(), TimeUnit.SECONDS);
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
            } catch (InterruptedException e) {
                LOGGER.error("Interrupted exception occurred", e);
            }
            assert sinkRecord != null;
            if (addedQueueMap.containsKey(sinkRecord.kafkaOffset())) {
                continue;
            }
            if (sinkRecord.value() instanceof Struct) {
                value = (Struct) sinkRecord.value();
            } else {
                value = null;
            }
            if (value == null) {
                // sink record of delete will bring a null record,the record offset add to sqlKafkaOffsets
                breakPointRecord.getReplayedOffset().add(sinkRecord.kafkaOffset());
                continue;
            }
            OgSinkProcessInfo.SINK_PROCESS_INFO.autoIncreaseExtractCount();
            DmlOperation dmlOperation = new DmlOperation(value);
            SourceField sourceField = new SourceField(value);
            Long lsn = sourceField.getLsn();
            Long kafkaOffset = sinkRecord.kafkaOffset();
            SinkRecordObject sinkRecordObject = new SinkRecordObject();
            sinkRecordObject.setDmlOperation(dmlOperation);
            sinkRecordObject.setSourceField(sourceField);
            sinkRecordObject.setKafkaOffset(kafkaOffset);
            if (filterByDb(sinkRecord, lsn, kafkaOffset)) {
                continue;
            }
            addedQueueMap.put(sinkRecord.kafkaOffset(), lsn);
            String schemaName = sourceField.getSchema();
            if (schemaMappingMap.get(schemaName) == null) {
                LOGGER.warn("Not specified schema [{}] mapping library relation.", schemaName);
                sqlErrCount++;
                long replayedCount = OgSinkProcessInfo.SINK_PROCESS_INFO.getReplayedCount();
                OgSinkProcessInfo.SINK_PROCESS_INFO.setReplayedCount(++replayedCount);
                String path = dmlOperation.getPath();
                if (!"".equals(path) && config.isDelCsv) {
                    if (!new File(path).delete()) {
                        LOGGER.warn("clear csv file failure.");
                    }
                }
                continue;
            }
            String tableName = sourceField.getTable();
            while (isWorkQueueBlock()) {
                try {
                    Thread.sleep(50);
                }
                catch (InterruptedException exp) {
                    LOGGER.warn("Receive interrupted exception while work queue block:{}", exp.getMessage());
                }
            }
            String tableFullName = schemaMappingMap.get(schemaName) + "." + tableName;
            findProperWorkThread(tableFullName, sinkRecordObject);
        }
    }

    private boolean filterByDb(SinkRecord sinkRecord, Long lsn, Long kafkaOffset) {
        if (isBpCondition && filterCount < BREAKPOINT_REPEAT_COUNT_LIMIT) {
            filterCount++;
            if (isSkipRecord(sinkRecord)) {
                LOGGER.info("The sinkRecord is already replay, "
                        + "so skip this txn that lsn is {}", lsn);
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
        } else {
            value = null;
        }
        if (value == null) {
            return false;
        }
        DmlOperation dmlOperation = new DmlOperation(value);
        SourceField sourceField = new SourceField(value);
        String schemaName = schemaMappingMap.get(sourceField.getSchema());
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
                } else if (sqlList.size() == 2) {
                    return sqlTools.isExistSql(sqlList.get(0)) && !sqlTools.isExistSql(sqlList.get(1));
                } else {
                    return false;
                }
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

    /**
     * if commit the same offset five times, will clear replayed offset queue
     *
     * @param offset offset
     */
    public void clearReplayedOffset(long offset) {
        breakPointRecord.getReplayedOffset().clear();
        addedQueueMap.clear();
        addedQueueMap.put(offset - 1, -1L);
    }

    private void findProperWorkThread(String tableFullName, SinkRecordObject sinkRecordObject) {
        if (runnableMap.containsKey(tableFullName)) {
            WorkThread workThread = threadList.get(runnableMap.get(tableFullName));
            workThread.addData(sinkRecordObject);
            return;
        }
        int relyThreadIndex = getRelyIndex(tableFullName);
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
                SinkRecordObject sinkRecordObject = null;
                String workThreadName = "";
                for (WorkThread workThread : threadList) {
                    if (workThread.getThreadSinkRecordObject() != null) {
                        if (sinkRecordObject == null || workThread.getThreadSinkRecordObject().getSourceField()
                                .getLsn() < sinkRecordObject.getSourceField().getLsn()) {
                            sinkRecordObject = workThread.getThreadSinkRecordObject();
                            workThreadName = workThread.getName();
                        }
                    }
                }
                if (sinkRecordObject != null) {
                    LOGGER.warn("[Breakpoint] {} in work thread {}",
                            sinkRecordObject.getSourceField().toString(), workThreadName);
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
        int size = 0;
        for (WorkThread workThread : threadList) {
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
            }
            else {
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

    private boolean isFreeBlock(ArrayList<WorkThread> threadList) {
        for (WorkThread workThread : threadList) {
            if (!workThread.isFreeBlock()) {
                return false;
            }
        }
        return true;
    }

    private void initFlowControl(OpengaussSinkConnectorConfig config) {
        maxQueueSize = config.maxQueueSize;
        openFlowControlThreshold = config.openFlowControlThreshold;
        closeFlowControlThreshold = config.closeFlowControlThreshold;
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

    private int[] getSuccessAndFailCount() {
        int successCount = 0;
        int failCount = sqlErrCount;
        for (WorkThread workThread : threadList) {
            successCount += workThread.getSuccessCount();
            failCount += workThread.getFailCount();
        }
        return new int[]{successCount, failCount, successCount + failCount};
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

    private void fullProgressReport() {
        if (ogFullSinkProcessInfo == null) {
            initFullProcess();
            return;
        }
        double complete = 0d;
        for (TableInfo table : tableList) {
            String tableFullName = schemaMappingMap.get(table.getSchema()) + "." + table.getName();
            if (!runnableMap.containsKey(tableFullName)) {
                continue;
            }
            WorkThread workThread = threadList.get(runnableMap.get(tableFullName));
            int count = workThread.processRecordMap.computeIfAbsent(tableFullName, k -> 0);
            table.setProcessRecord(count);
            BigDecimal decimal;
            table.updateStatus(ProgressStatus.IN_MIGRATED);
            if (table.getRecord() == 0) {
                decimal = new BigDecimal(0).setScale(1, BigDecimal.ROUND_HALF_UP);
                table.updateStatus(ProgressStatus.MIGRATED_COMPLETE);
            } else {
                decimal = new BigDecimal(count).divide(new BigDecimal(table.getRecord())).setScale(1,
                        BigDecimal.ROUND_HALF_UP);
                double data = (int) table.getData() == 0 ? 1 : table.getData();
                complete += decimal.floatValue() * data;
                if (count / table.getRecord() == 1) {
                    table.updateStatus(ProgressStatus.MIGRATED_COMPLETE);
                }
            }
            table.setPercent(decimal.floatValue());
        }
        TotalInfo totalInfo = ogFullSinkProcessInfo.getTotal();
        int time = (int) (totalInfo.getTime() + config.getCommitTimeInterval());
        BigDecimal divide = new BigDecimal(complete).divide(new BigDecimal(time)).setScale(2, BigDecimal.ROUND_HALF_UP);
        totalInfo.setSpeed(divide.doubleValue());
        totalInfo.setTime(time);
        ogFullSinkProcessInfo.setTotal(totalInfo);
        wirteFullToFile();
        boolean hasMatch = tableList.stream().anyMatch(o -> ((int) o.getPercent()) != 1);
        if (!hasMatch) {
            fullProgressReportService.shutdown();
            LOGGER.info("full migration complete. full report thread is close.");
        }
    }

    private void initFullProcess() {
        if (ogSinkFullCommiter.hasMessage()) {
            OgFullSourceProcessInfo ogFullSourceProcessInfo = ogSinkFullCommiter.getSourceFileJson();
            if (ogFullSourceProcessInfo.getTableList().isEmpty()) {
                return;
            }
            ogFullSinkProcessInfo = new OgFullSinkProcessInfo();
            tableList = ogFullSourceProcessInfo.getTableList().stream()
                    .filter(o -> schemaMappingMap.get(o.getSchema()) != null).collect(Collectors.toList());
            ogFullSinkProcessInfo.setTable(tableList);
            int record = 0;
            double data = 0d;
            int time = 0;
            double speed = 0d;
            for (TableInfo tableInfo : ogFullSourceProcessInfo.getTableList()) {
                record += tableInfo.getRecord();
                data += tableInfo.getData();
            }
            TotalInfo totalInfo = new TotalInfo(record, data, time, speed);
            ogFullSinkProcessInfo.setTotal(totalInfo);
            wirteFullToFile();
        }
    }

    private void wirteFullToFile() {
        ogSinkFullCommiter.commitSinkTableProcessInfo(ogFullSinkProcessInfo);
    }
}