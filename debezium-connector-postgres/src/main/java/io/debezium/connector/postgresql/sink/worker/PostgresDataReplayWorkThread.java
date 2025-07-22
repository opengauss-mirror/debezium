/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package io.debezium.connector.postgresql.sink.worker;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Locale;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.function.Function;

import org.apache.kafka.connect.errors.DataException;
import org.opengauss.copy.CopyManager;
import org.opengauss.core.BaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.postgresql.migration.MigrationUtil;
import io.debezium.connector.postgresql.migration.PostgresSqlConstant;
import io.debezium.connector.postgresql.process.ProgressInfo;
import io.debezium.connector.postgresql.process.ProgressStatus;
import io.debezium.connector.postgresql.sink.common.SourceDataField;
import io.debezium.connector.postgresql.sink.object.DataReplayOperation;
import io.debezium.connector.postgresql.sink.object.ConnectionInfo;
import io.debezium.connector.postgresql.sink.object.TableMetaData;
import io.debezium.connector.postgresql.sink.record.SinkDataRecord;
import io.debezium.connector.postgresql.sink.utils.ConnectionUtil;
import io.debezium.connector.postgresql.sink.utils.SqlTools;
import io.debezium.enums.ErrorCode;
import io.debezium.migration.BaseMigrationConfig;
import io.debezium.sink.worker.ReplayWorkThread;

/**
 * PostgresDataReplayWorkThread class
 *
 * @author jianghongbo
 * @since 2025/2/6
 */
public class PostgresDataReplayWorkThread extends ReplayWorkThread {
    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresDataReplayWorkThread.class);
    private static final String INSERT = "c";
    private static final String UPDATE = "u";
    private static final String DELETE = "d";
    private static final String TRUNCATE = "t";
    private static final String PATH = "p";
    private static final String INDEX = "i";
    private static final String SNAPSHOT = "ts";

    private final DateTimeFormatter sqlPattern = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH:mm:ss.SSS");
    private final SqlTools sqlTools;
    private final BlockingQueue<SinkDataRecord> sinkRecordQueue = new LinkedBlockingDeque<>();
    private final Map<String, TableMetaData> oldTableMap = new HashMap<>();
    private final Map<String, String> schemaMappingMap;
    private final List<String> failSqlList = new ArrayList<>();
    private final ConnectionInfo connectionInfo;
    private final boolean isCommitProcess;

    private Map<String, Integer> processRecordMap = new HashMap<>();
    private int successCount;
    private int failCount;
    private Connection connection;
    private Statement statement;
    private SinkDataRecord threadSinkRecordObject = null;
    private boolean isFreeBlock = true;
    private boolean isClearFile;
    private boolean isTransaction;
    private boolean isConnection = true;
    private boolean isStop = false;
    private boolean isRunning = true;
    private boolean shouldStop = false;
    private int index;
    private Map<String, Integer> tableSlicesMap = new HashMap<>();
    private Map<String, ProgressInfo> tableProcessMap;
    private long takeTime = 0L;
    private long copyTime = 0L;

    /**
     * Constructor
     *
     * @param schemaMappingMap Map<String, String> the schema mapping map
     * @param connectionInfo ConnectionInfo the connection information
     * @param sqlTools SqlTools the sql tools
     * @param index int the index
     * @param isCommitProcess boolean is commit progress
     * @param tableProcessMap Map<String, ProgressInfo> table progress
     */
    public PostgresDataReplayWorkThread(Map<String, String> schemaMappingMap, ConnectionInfo connectionInfo,
                                        SqlTools sqlTools, int index, boolean isCommitProcess,
                                        Map<String, ProgressInfo> tableProcessMap) {
        super(index);
        this.index = index;
        this.schemaMappingMap = schemaMappingMap;
        this.connectionInfo = connectionInfo;
        this.sqlTools = sqlTools;
        this.isTransaction = false;
        this.isCommitProcess = isCommitProcess;
        this.tableProcessMap = tableProcessMap;
    }

    @Override
    public void run() {
        SinkDataRecord sinkDataRecord = null;
        connection = ConnectionUtil.createConnection(connectionInfo);
        statement = createStatement(connection, 1);
        String sql = null;
        while (!isStop) {
            try {
                if (shouldStop && sinkRecordQueue.isEmpty()) {
                    LOGGER.warn("{} take time is {}, copy time is {}", currentThread().getName(), takeTime, copyTime);
                    LOGGER.info("PostgresDataReplayWorkThread-{} exiting", index);
                    break;
                }
                long start = System.currentTimeMillis();
                sinkDataRecord = sinkRecordQueue.take();
                takeTime += System.currentTimeMillis() - start;
                sql = constructSql(sinkDataRecord);
                if (!isConnection) {
                    break;
                }
                if ("".equals(sql)) {
                    continue;
                }
                LOGGER.debug(sql);
                statement.executeUpdate(sql);
                successCount++;
                threadSinkRecordObject = sinkDataRecord;
            } catch (SQLException exp) {
                failCount++;
                updateConnectionAndExecuteSql(sql, sinkDataRecord);
            } catch (InterruptedException exp) {
                LOGGER.warn("Interrupted exception occurred", exp);
            } catch (DataException exp) {
                failCount++;
                if (sinkDataRecord != null && isIncrementalData(sinkDataRecord)) {
                    String tableFullName = schemaMappingMap.get(sinkDataRecord.getSourceField()
                        .getSchema()) + "." + sinkDataRecord.getSourceField().getTable();
                    oldTableMap.remove(tableFullName);
                    LOGGER.error("{}DataException occurred because of invalid field, possible reason is tables of "
                        + "openGauss and Postgresql have same table name {} but different table structure.",
                        ErrorCode.DATA_CONVERT_EXCEPTION, tableFullName, exp);
                    return;
                }
                LOGGER.error("{}DataException occurred because of invalid field, possible reason is tables "
                        + "of openGauss and Postgresql have same table name but different table structure.",
                    ErrorCode.DATA_CONVERT_EXCEPTION, exp);
            }
        }
    }

    /**
     * Adds data
     *
     * @param sinkRecordObject SinkRecordObject the sinkRecordObject
     */
    public void addData(SinkDataRecord sinkRecordObject) {
        sinkRecordQueue.add(sinkRecordObject);
    }

    private boolean isIncrementalData(SinkDataRecord sinkDataRecord) {
        return BaseMigrationConfig.MessageType.INCREMENTALDATA.equals(sinkDataRecord.getMsgType());
    }

    /**
     * Constructs sql
     *
     * @param sinkDataRecord SinkDataRecord the sinkDataRecord
     * @return String the sql
     */
    public String constructSql(SinkDataRecord sinkDataRecord) {
        SourceDataField sourceField = sinkDataRecord.getSourceField();
        DataReplayOperation dataReplayOperation = sinkDataRecord.getDataReplayOperation();
        String schemaName = schemaMappingMap.getOrDefault(sourceField.getSchema(), sourceField.getSchema());
        String tableFullName = schemaName + "." + sourceField.getTable();
        TableMetaData tableMetaData = null;
        String sql = "";
        if (isIncrementalData(sinkDataRecord)) {
            if (oldTableMap.containsKey(tableFullName)) {
                tableMetaData = oldTableMap.get(tableFullName);
            } else {
                tableMetaData = sqlTools.getTableMetaData(schemaName, sourceField.getTable());
                oldTableMap.put(tableFullName, tableMetaData);
            }
            if (tableMetaData == null && !sqlTools.getIsConnection()) {
                isConnection = false;
                LOGGER.error("There is a connection problem with opengauss,"
                        + " check the database status or connection");
                return sql;
            }
        }
        String operation = dataReplayOperation.getOperation();
        if (PATH.equals(operation) && tableSlicesMap.get(tableFullName) == null) {
            tableSlicesMap.put(tableFullName, 0);
        }
        switch (operation) {
            case TRUNCATE:
                processFullPrefix(sinkDataRecord, tableFullName);
                break;
            case PATH:
                processFullData(dataReplayOperation, tableFullName);
                break;
            case INSERT:
                sql = sqlTools.getInsertSql(tableMetaData, dataReplayOperation.getAfter());
                break;
            case UPDATE:
                sql = sqlTools.getUpdateSql(tableMetaData, dataReplayOperation.getBefore(),
                        dataReplayOperation.getAfter());
                break;
            case DELETE:
                sql = sqlTools.getDeleteSql(tableMetaData, dataReplayOperation.getBefore());
                break;
            case INDEX:
                sqlTools.replayIndex(dataReplayOperation, sinkDataRecord, schemaName);
                break;
            case SNAPSHOT:
                sql = sqlTools.getTableRecordSnapshotSql(sinkDataRecord);
                break;
            default:
                break;
        }
        return sql;
    }

    /**
     * set is clear file
     *
     * @param isDelCsv boolean
     */
    public void setClearFile(boolean isDelCsv) {
        isClearFile = isDelCsv;
    }

    private void processFullPrefix(SinkDataRecord sinkDataRecord, String tableFullName) {
        String truncateSql = String.format("truncate table %s;", tableFullName);
        try {
            statement.executeUpdate(truncateSql);
        } catch (SQLException sqlException) {
            printSqlException(sinkDataRecord.getSourceField().toString(), sqlException, truncateSql);
        }
    }

    private void processFullData(DataReplayOperation dataReplayOperation, String tableFullName) {
        String path = dataReplayOperation.getPath();
        Integer sliceNumber = dataReplayOperation.getSliceNumber();
        Integer totalSlice = dataReplayOperation.getTotalSlice();
        Double spacePerSlice = dataReplayOperation.getSpacePerSlice();
        long row = 0L;
        try {
            if ("".equals(path) && sliceNumber > 0) {
                if (isCommitProcess) {
                    computeProcess(tableFullName, 1f, spacePerSlice, sliceNumber,
                            ProgressStatus.MIGRATED_COMPLETE, "", row);
                }
                LOGGER.info("{}: Finish copy table {}, total {} slices, success {} slices", getName(), tableFullName,
                        sliceNumber, tableSlicesMap.get(tableFullName));
                tableSlicesMap.remove(tableFullName);
                return;
            }

            CopyManager copyManager = new CopyManager((BaseConnection) connection);
            FileReader csvReader = new FileReader(path);
            long start = System.currentTimeMillis();
            row = copyManager.copyIn(String.format(Locale.ROOT, PostgresSqlConstant.COPY_SQL, tableFullName),
                    csvReader);
            copyTime += System.currentTimeMillis() - start;
            if (isCommitProcess) {
                if (sliceNumber > totalSlice) {
                    LOGGER.error("percent error: sliceNumber: {}, totalSlice:{}", sliceNumber, totalSlice);
                }
                float percent = sliceNumber > totalSlice ? 0.99f : ((float) sliceNumber / totalSlice);
                computeProcess(tableFullName, percent, spacePerSlice, sliceNumber, ProgressStatus.IN_MIGRATED, "",
                        row);
            }
            tableSlicesMap.put(tableFullName, tableSlicesMap.get(tableFullName) + 1);
            printCopyProcess(sliceNumber, tableFullName, row);
            clearCsvFile(path);
        } catch (SQLException | IOException e) {
            if (isCommitProcess) {
                float percent = sliceNumber > totalSlice ? 0.99f : ((float) sliceNumber / totalSlice);
                computeProcess(tableFullName, percent, spacePerSlice, sliceNumber, ProgressStatus.MIGRATED_FAILURE,
                        "", row);
            }
            LOGGER.error("{}: COPY {} slices for table {} failed, csv file is {}:", getName(), sliceNumber,
                    tableFullName, path, e);
        }
    }

    private void printCopyProcess(Integer sliceNumber, String tableFullName, long row) {
        if (sliceNumber % MigrationUtil.LOG_REPORT_INTERVAL_SLICES == 0) {
            LOGGER.info("{}: COPY {} slice for table {} success, {} lines per slice, total success {} slices",
                    getName(), sliceNumber, tableFullName, row, tableSlicesMap.get(tableFullName));
        }
    }

    private void computeProcess(String tableFullName, Float percent, Double spacePerSlice, Integer sliceNumber,
                                ProgressStatus status, String error, long row) {
        ProgressInfo progressInfo = tableProcessMap.get(tableFullName) == null ? new ProgressInfo()
                : tableProcessMap.get(tableFullName);
        progressInfo.setName(tableFullName);
        BigDecimal precisionPercent = new BigDecimal(percent).setScale(2, RoundingMode.HALF_UP);
        progressInfo.setPercent(precisionPercent.floatValue());
        ProgressStatus realStatus = status;
        int preStatusCode = progressInfo.getStatus();
        if (preStatusCode == ProgressStatus.MIGRATED_FAILURE.getCode()) {
            realStatus = ProgressStatus.MIGRATED_FAILURE;
        }
        progressInfo.setStatus(realStatus.getCode());
        progressInfo.setError(error);
        BigDecimal precisionData = new BigDecimal(spacePerSlice * sliceNumber).setScale(2,
                RoundingMode.HALF_UP);
        progressInfo.setData(precisionData.doubleValue());
        progressInfo.setRecord(progressInfo.getRecord() + row);
        tableProcessMap.put(tableFullName, progressInfo);
    }

    private void clearCsvFile(String path) {
        if (!isClearFile) {
            return;
        }
        File file = new File(path);
        if (!file.delete()) {
            LOGGER.info("clear csv file failure.");
        }
    }

    /**
     * get fail sql list
     *
     * @return List the fail sql list
     */
    public List<String> getFailSqlList() {
        return failSqlList;
    }

    /**
     * get success count
     *
     * @return count of replayed successfully
     */
    public int getSuccessCount() {
        return this.successCount;
    }

    /**
     * clear fail sql list
     */
    public void clearFailSqlList() {
        failSqlList.clear();
    }

    private void updateConnectionAndExecuteSql(String sql, SinkDataRecord sinkDataRecord) {
        try {
            statement.close();
            connection.close();
            connection = ConnectionUtil.createConnection(connectionInfo);
            statement = connection.createStatement();
            statement.executeUpdate(sql);
            successCount++;
        } catch (SQLException exp) {
            failCount++;
            if (!connectionInfo.checkConnectionStatus(connection)) {
                return;
            }
            printSqlException(sinkDataRecord.getSourceField().toString(), exp, sql);
        }
    }

    private void printSqlException(String data, SQLException exp, String sql) {
        failSqlList.add("-- "
                + sqlPattern.format(LocalDateTime.now())
                + ": "
                + data
                + System.lineSeparator()
                + "-- "
                + exp.getMessage().replaceAll(System.lineSeparator(), "; ")
                + System.lineSeparator()
                + sql
                + System.lineSeparator());
        LOGGER.error("SQL exception occurred in data {}", data);
        LOGGER.error("The error SQL statement executed is: {}", sql);
        LOGGER.error("the cause of the exception is {}", exp.getMessage());
    }

    /**
     * get fail count
     *
     * @return int the fail count
     */
    public int getFailCount() {
        return failCount;
    }

    /**
     * Get thread sink record object
     *
     * @return SinkRecordObject get sink record object
     */
    public SinkDataRecord getThreadSinkRecordObject() {
        return threadSinkRecordObject;
    }

    /**
     * Get work thread queue length
     *
     * @return int the queue length in work thread
     */
    public int getQueueLength() {
        return sinkRecordQueue.size();
    }

    /**
     * Gets connection.
     *
     * @return the value of connection
     */
    public Connection getConnection() {
        return connection;
    }

    /**
     * Sets the connection.
     *
     * @param connection the connection
     */
    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    /**
     * Gets isFreeBlock
     *
     * @return boolean the isFreeBlock
     */
    public boolean isFreeBlock() {
        return isFreeBlock;
    }

    /**
     * Sets isFreeBlock
     *
     * @param isFreeBlock boolean the isFreeBlock
     */
    public void setFreeBlock(boolean isFreeBlock) {
        this.isFreeBlock = isFreeBlock;
    }

    /**
     * Sets isStop
     *
     * @param isStop boolean
     */
    public void setIsStop(boolean isStop) {
        this.isStop = isStop;
    }

    /**
     * Gets
     *
     * @return boolean isRunning
     */
    public boolean isRunning() {
        return isRunning;
    }

    /**
     * Sets isRunning
     *
     * @param isRunning boolean
     */
    public void setRunning(boolean isRunning) {
        this.isRunning = isRunning;
    }

    /**
     * Gets
     *
     * @return boolean shouldStop
     */
    public boolean isShouldStop() {
        return shouldStop;
    }

    /**
     * Sets shouldStop
     *
     * @param shouldStop boolean
     */
    public void setShouldStop(boolean shouldStop) {
        this.shouldStop = shouldStop;
    }

    /**
     * get record count
     *
     * @param stringFullName String
     * @param function Function
     * @return int
     */
    public int getRecordCount(String stringFullName, Function function) {
        return processRecordMap.computeIfAbsent(stringFullName, function);
    }

    /**
     * Gets
     *
     * @return Map<String, ProgressInfo> tableProcessMap
     */
    public Map<String, ProgressInfo> getTableProcessMap() {
        return tableProcessMap;
    }
}
