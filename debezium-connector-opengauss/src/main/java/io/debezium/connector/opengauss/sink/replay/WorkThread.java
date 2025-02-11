/**
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.opengauss.sink.replay;

import com.mysql.cj.jdbc.ClientPreparedStatement;
import com.mysql.cj.jdbc.exceptions.CommunicationsException;

import io.debezium.ThreadExceptionHandler;
import io.debezium.connector.breakpoint.BreakPointObject;
import io.debezium.connector.breakpoint.BreakPointRecord;
import io.debezium.connector.opengauss.sink.object.ColumnMetaData;
import io.debezium.connector.opengauss.sink.object.DataOperation;
import io.debezium.connector.opengauss.sink.object.DdlOperation;
import io.debezium.connector.opengauss.sink.object.SourceField;
import io.debezium.connector.opengauss.sink.object.SinkRecordObject;
import io.debezium.connector.opengauss.sink.object.ConnectionInfo;
import io.debezium.connector.opengauss.sink.object.DmlOperation;
import io.debezium.connector.opengauss.sink.object.TableMetaData;
import io.debezium.connector.opengauss.sink.utils.SqlTools;
import io.debezium.enums.ErrorCode;

import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.data.Struct;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.stream.Collectors;

/**
 * Description: WorkThread class
 *
 * @author wangzhengyuan
 * @since 2022-11-29
 */
public class WorkThread extends Thread {
    private static final Logger LOGGER = LoggerFactory.getLogger(WorkThread.class);
    private static final String INSERT = "c";
    private static final String UPDATE = "u";
    private static final String DELETE = "d";
    private static final String TRUNCATE = "t";
    private static final String PATH = "p";
    private static final String DELIMITER = System.lineSeparator();

    private final DateTimeFormatter sqlPattern = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH:mm:ss.SSS");
    private final SqlTools sqlTools;
    private final ConnectionInfo connectionInfo;
    private final BlockingQueue<SinkRecordObject> sinkRecordQueue = new LinkedBlockingDeque<>();
    private final Map<String, TableMetaData> oldTableMap = new HashMap<>();
    private final Map<String, String> schemaMappingMap;
    private final List<String> failSqlList = new ArrayList<>();
    Map<String, Integer> processRecordMap = new HashMap<>();
    private BreakPointRecord breakPointRecord;
    private PriorityBlockingQueue<Long> replayedOffsets;
    private int successCount;
    private int fullSuccessCount;
    private int failCount;
    private int fullFailCount;
    private Connection connection;
    private Statement statement;
    private SinkRecordObject threadSinkRecordObject = null;
    private boolean isFreeBlock = true;
    private ClientPreparedStatement clientPreparedStatement;
    private CopyManager copyManager;
    private boolean isClearFile;
    private boolean isTransaction;
    private boolean isConnection = true;
    private boolean isStop = false;
    private final Map<String, List<ColumnMetaData>> fullTableColumnMap = new HashMap<>();

    /**
     * Constructor
     *
     * @param schemaMappingMap Map<String, String> the schema mapping map
     * @param connectionInfo ConnectionInfo the connection information
     * @param sqlTools SqlTools the sql tools
     * @param index int the index
     * @param breakPointRecord record breakpoint info
     */
    public WorkThread(Map<String, String> schemaMappingMap, ConnectionInfo connectionInfo,
                      SqlTools sqlTools, int index, BreakPointRecord breakPointRecord) {
        super("work-thread-" + index);
        this.schemaMappingMap = schemaMappingMap;
        this.connectionInfo = connectionInfo;
        this.sqlTools = sqlTools;
        this.breakPointRecord = breakPointRecord;
        this.replayedOffsets = breakPointRecord.getReplayedOffset();
        this.isTransaction = false;
    }

    @Override
    public void run() {
        Thread.currentThread().setUncaughtExceptionHandler(new ThreadExceptionHandler());
        SinkRecordObject sinkRecordObject = null;
        connection = createConnection();
        try {
            statement = connection.createStatement();
        } catch (SQLException exp) {
            LOGGER.error("{}SQL exception occurred in work thread", ErrorCode.SQL_EXCEPTION, exp);
        }
        String sql = null;
        while (!isStop) {
            try {
                sinkRecordObject = sinkRecordQueue.take();
                sql = constructSql(sinkRecordObject);
                if (!isConnection) {
                    break;
                }
                if ("".equals(sql)) {
                    replayedOffsets.offer(sinkRecordObject.getKafkaOffset());
                    continue;
                }
                if (clientPreparedStatement != null || copyManager != null) {
                    updateConnectionAndExecuteSql(sql, sinkRecordObject);
                    continue;
                }
                LOGGER.debug(sql);
                statement.executeUpdate(sql);
                successCount++;
                threadSinkRecordObject = sinkRecordObject;
                replayedOffsets.offer(sinkRecordObject.getKafkaOffset());
                savedBreakPointInfo(sinkRecordObject, false);
            } catch (CommunicationsException exp) {
                updateConnectionAndExecuteSql(sql, sinkRecordObject);
            } catch (SQLException exp) {
                failCount++;
                printSqlException(sinkRecordObject.getSourceField().toString(), exp, sql);
            } catch (InterruptedException exp) {
                LOGGER.warn("Interrupted exception occurred", exp);
            } catch (DataException exp) {
                failCount++;
                if (sinkRecordObject != null) {
                    oldTableMap.remove(schemaMappingMap.get(sinkRecordObject.getSourceField()
                            .getSchema()) + "." + sinkRecordObject.getSourceField().getTable());
                }
                LOGGER.error("{}DataException occurred because of invalid field, possible reason is tables "
                        + "of OpenGauss and MySQL have same table name but different table structure.",
                    ErrorCode.DATA_CONVERT_EXCEPTION, exp);
            }
        }
    }

    /**
     * Adds data
     *
     * @param sinkRecordObject SinkRecordObject the sinkRecordObject
     */
    public void addData(SinkRecordObject sinkRecordObject) {
        sinkRecordQueue.add(sinkRecordObject);
    }

    /**
     * Constructs sql
     *
     * @param sinkRecordObject SinkRecordObject the sinkRecordObject
     * @return String the sql
     */
    public String constructSql(SinkRecordObject sinkRecordObject) {
        SourceField sourceField = sinkRecordObject.getSourceField();
        DataOperation dataOperation = sinkRecordObject.getDataOperation();
        if (dataOperation instanceof DdlOperation) {
            DdlOperation ddlOperation = (DdlOperation) dataOperation;
            String fullName = ddlOperation.getFullName();
            oldTableMap.remove(fullName);
            return ddlOperation.getDdl();
        }
        String schemaName = schemaMappingMap.getOrDefault(sourceField.getSchema(), sourceField.getSchema());
        String tableFullName = schemaName + "." + sourceField.getTable();
        TableMetaData tableMetaData;
        if (oldTableMap.containsKey(tableFullName)) {
            tableMetaData = oldTableMap.get(tableFullName);
        } else {
            tableMetaData = sqlTools.getTableMetaData(schemaName, sourceField.getTable());
            oldTableMap.put(tableFullName, tableMetaData);
        }
        String sql = "";
        if (tableMetaData == null && !sqlTools.getIsConnection()) {
            isConnection = false;
            LOGGER.error("{}There is a connection problem with the mysql,"
                    + " check the database status or connection", ErrorCode.DB_CONNECTION_EXCEPTION);
            return sql;
        }
        String operation = dataOperation.getOperation();
        DmlOperation dmlOperation = (DmlOperation) dataOperation;
        switch (operation) {
            case TRUNCATE:
                processFullPrefix(tableMetaData, sinkRecordObject);
                break;
            case PATH:
                processFullData(dmlOperation, tableFullName, tableMetaData, sinkRecordObject);
                break;
            case INSERT:
                sql = sqlTools.getInsertSql(tableMetaData, dmlOperation.getAfter());
                break;
            case UPDATE:
                sql = sqlTools.getUpdateSql(tableMetaData, dmlOperation.getBefore(), dmlOperation.getAfter());
                break;
            case DELETE:
                sql = sqlTools.getDeleteSql(tableMetaData, dmlOperation.getBefore());
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

    private void processFullPrefix(TableMetaData tableMetaData, SinkRecordObject sinkRecordObject) {
        StringBuilder sb = new StringBuilder();
        sb.append("truncate table ")
                .append(sqlTools.getTableFullName(tableMetaData)).append(";");
        try {
            statement.executeUpdate(sb.toString());
            fullSuccessCount++;
            savedBreakPointInfo(sinkRecordObject, true);
        } catch (CommunicationsException exp) {
            try {
                connection = createConnection();
                statement = connection.createStatement();
                statement.executeUpdate(sb.toString());
                savedBreakPointInfo(sinkRecordObject, true);
                fullSuccessCount++;
            } catch (SQLException sqlException) {
                fullFailCount++;
                if (!connectionInfo.checkConnectionStatus(connection)) {
                    return;
                }
                printSqlException(sinkRecordObject.getSourceField().toString(), sqlException, sb.toString());
            }
        } catch (SQLException sqlException) {
            fullFailCount++;
            printSqlException(sinkRecordObject.getSourceField().toString(), sqlException, sb.toString());
        }
    }

    private void processFullData(DmlOperation dmlOperation, String tableFullName, TableMetaData tableMetaData,
                                 SinkRecordObject sinkRecordObject) {
        String columnString = dmlOperation.getColumnString();
        String loadSql = sqlTools.loadFullSql(tableFullName);
        if (loadSql == null) {
            return;
        }
        loadSql = sqlTools.sqlAddBitCast(tableMetaData, columnString, loadSql);
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(loadSql);
            if (clientPreparedStatement == null
                    && preparedStatement.isWrapperFor(com.mysql.cj.jdbc.JdbcStatement.class)) {
                statement.execute("set session SQL_LOG_BIN=0;");
                statement.execute("set session UNIQUE_CHECKS=0;");
                clientPreparedStatement = preparedStatement.unwrap(ClientPreparedStatement.class);
            } else if (copyManager == null
                    && preparedStatement.isWrapperFor(org.postgresql.jdbc.PgStatement.class)) {
                copyManager = new CopyManager((BaseConnection) connection);
            } else {
                LOGGER.info("Type for preparedStatement is {}", preparedStatement);
            }
        } catch (SQLException e) {
            LOGGER.error("{}workthread exception", ErrorCode.SQL_EXCEPTION, e);
        }
        List<String> lineList = new ArrayList<>();
        String path = dmlOperation.getPath();
        try {
            lineList = Files.lines(Paths.get(path))
                    .flatMap(line -> Arrays.stream(line.split(System.lineSeparator())))
                    .collect(Collectors.toList());
        } catch (IOException e) {
            LOGGER.error("{}load csv file failure IO exception", ErrorCode.IO_EXCEPTION, e);
        }
        Struct after = dmlOperation.getAfter();
        List<ColumnMetaData> columnList = getColumnList(tableFullName, tableMetaData, columnString);
        List<String> list = sqlTools.conversionFullData(columnList, lineList, after);
        loadData(path, list, loadSql, tableFullName, sinkRecordObject);
    }

    private List<ColumnMetaData> getColumnList(String tableFullName, TableMetaData tableMetaData, String columnString) {
        if (fullTableColumnMap.containsKey(tableFullName)) {
            return fullTableColumnMap.get(tableFullName);
        }
        List<ColumnMetaData> columnList = tableMetaData.getColumnList();
        List<ColumnMetaData> columnMetaDataList = new ArrayList<>();
        String[] columns = columnString.split(",");
        for (String column : columns) {
            for (ColumnMetaData columnMetaData : columnList) {
                if (columnMetaData.getColumnName().equals(column)) {
                    columnMetaDataList.add(columnMetaData);
                }
            }
        }
        fullTableColumnMap.put(tableFullName, columnMetaDataList);
        return columnMetaDataList;
    }

    private void loadData(String path, List<String> list, String sql, String tableName,
                          SinkRecordObject sinkRecordObject) {
        ByteArrayInputStream inputStream = getInputStream(list);
        int count = processRecordMap.computeIfAbsent(tableName, k -> 0);
        try {
            connection.setAutoCommit(false);
            if (clientPreparedStatement != null) {
                clientPreparedStatement.setLocalInfileInputStream(inputStream);
                clientPreparedStatement.executeUpdate(sql);
            } else if (copyManager != null) {
                copyManager.copyIn(sql, inputStream);
            } else {
                LOGGER.error("Unknown statement type '{}' for loadData.", statement);
            }
            connection.commit();
            fullSuccessCount++;
            processRecordMap.put(tableName, count + list.size());
            replayedOffsets.offer(sinkRecordObject.getKafkaOffset());
            savedBreakPointInfo(sinkRecordObject, true);
            clearCsvFile(path);
        } catch (CommunicationsException exp) {
            LOGGER.error("{}statement closed unexpectedly.", ErrorCode.DB_CONNECTION_EXCEPTION);
            retryLoad(sql, inputStream, sinkRecordObject);
            if (isConnection) {
                processRecordMap.put(tableName, count + list.size());
            }
        } catch (SQLException | IOException e) {
            if (!connectionInfo.checkConnectionStatus(connection)) {
                return;
            }
            fullFailCount++;
            printSqlException(path, e, sql);
        } finally {
            try {
                inputStream.close();
            } catch (IOException e) {
                LOGGER.error("{}inputStream close error", ErrorCode.IO_EXCEPTION, e);
            }
        }
    }

    private void retryLoad(String sql, InputStream ins, SinkRecordObject sinkRecordObject) {
        try {
            clientPreparedStatement.close();
            connection.close();
            statement = connection.createStatement();
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            if (preparedStatement.isWrapperFor(com.mysql.cj.jdbc.JdbcStatement.class)) {
                clientPreparedStatement = preparedStatement.unwrap(ClientPreparedStatement.class);
                clientPreparedStatement.setLocalInfileInputStream(ins);
                clientPreparedStatement.executeUpdate();
                fullSuccessCount++;
            } else if (preparedStatement.isWrapperFor(org.postgresql.jdbc.PgStatement.class)) {
                copyManager.copyIn(sql, ins);
            } else {
                LOGGER.info("Type for preparedStatement is {}", preparedStatement);
            }
            replayedOffsets.offer(sinkRecordObject.getKafkaOffset());
            savedBreakPointInfo(sinkRecordObject, false);
        } catch (SQLException | IOException e) {
            if (!connectionInfo.checkConnectionStatus(connection)) {
                return;
            }
            LOGGER.error("{}SQL exception occurred in work thread.", ErrorCode.IO_EXCEPTION, e);
        }
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

    private ByteArrayInputStream getInputStream(List<String> data) {
        String dataStr = String.join(DELIMITER, data) + DELIMITER;
        byte[] bytes = dataStr.getBytes(StandardCharsets.UTF_8);
        return new ByteArrayInputStream(bytes);
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
     * get full success count
     *
     * @return count of replayed successfully
     */
    public int getFullSuccessCount() {
        return this.fullSuccessCount;
    }

    /**
     * clear fail sql list
     */
    public void clearFailSqlList() {
        failSqlList.clear();
    }

    private void updateConnectionAndExecuteSql(String sql, SinkRecordObject sinkRecordObject) {
        try {
            if (clientPreparedStatement != null) {
                clientPreparedStatement.close();
                clientPreparedStatement = null;
            }
            if (copyManager != null) {
                copyManager = null;
            }
            statement.close();
            connection.close();
            connection = createConnection();
            statement = connection.createStatement();
            statement.executeUpdate(sql);
            savedBreakPointInfo(sinkRecordObject, false);
            successCount++;
        } catch (SQLException exp) {
            failCount++;
            if (!connectionInfo.checkConnectionStatus(connection)) {
                return;
            }
            printSqlException(sinkRecordObject.getSourceField().toString(), exp, sql);
        }
    }

    private void printSqlException(String data, Exception exp, String sql) {
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
        LOGGER.error("{}SQL exception occurred in data {}", ErrorCode.SQL_EXCEPTION, data);
        LOGGER.error("{}The error SQL statement executed is: {}", ErrorCode.SQL_EXCEPTION, sql);
        LOGGER.error("{}The cause of the exception is {}", ErrorCode.SQL_EXCEPTION, exp.getMessage());
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
     * get full fail count
     *
     * @return int the fail count
     */
    public int getFullFailCount() {
        return fullFailCount;
    }

    /**
     * Get thread sink record object
     *
     * @return SinkRecordObject get sink record object
     */
    public SinkRecordObject getThreadSinkRecordObject() {
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

    public void setIsStop(boolean isStop) {
        this.isStop = isStop;
    }

    /**
     * Save breakpoint data to kafka
     *
     * @param sinkRecordObject sink record object
     */
    private void savedBreakPointInfo(SinkRecordObject sinkRecordObject, boolean isFull) {
        BreakPointObject openGaussBpObject = new BreakPointObject();
        SourceField sourceField = sinkRecordObject.getSourceField();
        Long lsn = sourceField.getLsn();
        Long kafkaOffset = sinkRecordObject.getKafkaOffset();
        openGaussBpObject.setBeginOffset(kafkaOffset);
        openGaussBpObject.setLsn(lsn);
        openGaussBpObject.setTimeStamp(LocalDateTime.now().toString());
        breakPointRecord.storeRecord(openGaussBpObject, isTransaction, isFull);
    }

    /**
     * Create connection by name
     *
     * @return connection
     */
    private Connection createConnection() {
        if ("mysql".equals(connectionInfo.getDatabaseType().toLowerCase(Locale.ROOT))) {
            return connectionInfo.createMysqlConnection();
        } else if ("oracle".equals(connectionInfo.getDatabaseType().toLowerCase(Locale.ROOT))) {
            return connectionInfo.createOracleConnection();
        } else if ("postgres".equals(connectionInfo.getDatabaseType().toLowerCase(Locale.ROOT))) {
            return connectionInfo.createPostgresConnection();
        } else {
            return connectionInfo.createOpenGaussConnection();
        }
    }
}