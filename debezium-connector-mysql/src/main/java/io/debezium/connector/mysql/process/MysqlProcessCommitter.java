/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.process;

import java.io.File;
import java.sql.SQLException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.sink.task.MySqlSinkConnectorConfig;
import io.debezium.connector.process.BaseProcessCommitter;
import io.debezium.connector.process.BaseSourceProcessInfo;

/**
 * Description: MysqlProcessCommitter
 *
 * @author wangzhengyuan
 * @since  2023-03-20
 */
public class MysqlProcessCommitter extends BaseProcessCommitter {
    private static final Logger LOGGER = LoggerFactory.getLogger(MysqlProcessCommitter.class);
    private static final String SHOW_MASTER_STATUS = "SHOW MASTER STATUS";
    private static final String FORWARD_SOURCE_PROCESS_PREFIX = "forward-source-process-";
    private static final String FORWARD_SINK_PROCESS_PREFIX = "forward-sink-process-";
    private static final String CREATE_COUNT_INFO_NAME = "start-event-index.txt";
    private static final String GTID = "Executed_Gtid_Set";

    private final ThreadPoolExecutor threadPool = new ThreadPoolExecutor(1, 1, 100,
            TimeUnit.SECONDS, new LinkedBlockingQueue<>(1));
    private BaseSourceProcessInfo sourceProcessInfo;
    private MysqlSinkProcessInfo sinkProcessInfo;
    private MySqlConnection mysqlConnection;
    private long createCount;
    private String[] gtidSet;
    private boolean isParallelBasedTransaction;

    /**
     * Constructor
     *
     * @param connectorConfig MySqlConnectorConfig the mySqlConnectorConfig
     * @param originGtidSet String the origin gtid set
     * @param connection MySqlConnection the connection
     */
    public MysqlProcessCommitter(MySqlConnectorConfig connectorConfig, String originGtidSet,
                                 MySqlConnection connection) {
        super(connectorConfig, FORWARD_SOURCE_PROCESS_PREFIX);
        this.fileFullPath = initFileFullPath(file + File.separator + FORWARD_SOURCE_PROCESS_PREFIX);
        this.currentFile = new File(fileFullPath);
        this.isAppendWrite = connectorConfig.appendWrite();
        deleteRedundantFiles(connectorConfig.filePath(),
                connectorConfig.processFileCountLimit(), connectorConfig.processFileTimeLimit());
        this.mysqlConnection = connection;
        this.isParallelBasedTransaction = connectorConfig.shouldProvideTransactionMetadata();
        if (isParallelBasedTransaction) {
            this.sourceProcessInfo = BaseSourceProcessInfo.TRANSACTION_SOURCE_PROCESS_INFO;
        }
        else {
            this.sourceProcessInfo = BaseSourceProcessInfo.TABLE_SOURCE_PROCESS_INFO;
        }
        initOriginGtidSet(originGtidSet.split(","));
        executeOutPutThread(connectorConfig.createCountInfoPath() + File.separator);
    }

    private void initOriginGtidSet(String[] originGtidSets) {
        String[] currentGtidSets = getCurrentGtid();
        gtidSet = new String[currentGtidSets.length];
        for (int i = 0; i < gtidSet.length; i++) {
            for (int j = 0; j < originGtidSets.length; j++) {
                if (getUuid(originGtidSets[j]).equals(getUuid(currentGtidSets[i]))) {
                    gtidSet[i] = originGtidSets[j];
                    break;
                }
            }
        }
    }

    private String getUuid(String gtid) {
        return gtid.split(":")[0];
    }

    /**
     * Constructor
     *
     * @param connectorConfig MySqlSinkConnectorConfig the connectorConfig
     */
    public MysqlProcessCommitter(MySqlSinkConnectorConfig connectorConfig) {
        super(connectorConfig, FORWARD_SINK_PROCESS_PREFIX);
        this.fileFullPath = initFileFullPath(file + File.separator + FORWARD_SINK_PROCESS_PREFIX);
        this.currentFile = new File(fileFullPath);
        this.isAppendWrite = connectorConfig.isAppend();
        this.createCountInfoPath = connectorConfig.getCreateCountInfoPath();
        this.isParallelBasedTransaction = connectorConfig.isParallelBasedTransaction;
        sinkProcessInfo = MysqlSinkProcessInfo.SINK_PROCESS_INFO;
        deleteRedundantFiles(connectorConfig.getSinkProcessFilePath(),
                connectorConfig.getProcessFileCountLimit(), connectorConfig.getProcessFileTimeLimit());
    }

    /**
     * Constructor
     *
     * @param failSqlPath String the fail sql path
     * @param fileSize int the file size
     */
    public MysqlProcessCommitter(String failSqlPath, int fileSize) {
        super(failSqlPath, fileSize);
    }

    /**
     * statSourceProcessInfo
     *
     * @return MysqlSourceProcessInfo the mysqlSourceProcessInfo
     */
    protected String statSourceProcessInfo() {
        long before = waitTimeInterval(true);
        sourceProcessInfo.setSpeed(before, commitTimeInterval);
        if (isParallelBasedTransaction) {
            refreshCreateCount();
            sourceProcessInfo.setCreateCount(createCount);
        }
        sourceProcessInfo.setRest();
        sourceProcessInfo.setTimestamp();
        return sourceProcessInfo.toString();
    }

    private void refreshCreateCount() {
        String[] currentGtidSets = getCurrentGtid();
        for (int i = 0; i < gtidSet.length; i++) {
            if (!gtidSet[i].equals(currentGtidSets[i])) {
                createCount += getSliceCreateCount(gtidSet[i], currentGtidSets[i]);
            }
        }
        gtidSet = currentGtidSets;
    }

    private long getSliceCreateCount(String before, String after) {
        return getEventIndex(after) - getEventIndex(before);
    }

    private long getEventIndex(String gtid) {
        String tid = gtid.split(":")[gtid.split(":").length - 1];
        return Long.parseLong(tid.split("-")[tid.split("-").length - 1]);
    }

    /**
     * statSinkProcessInfo
     *
     * @return MysqlSinkProcessInfo the mysqlSinkProcessInfo
     */
    protected String statSinkProcessInfo() {
        long before = waitTimeInterval(false);
        sinkProcessInfo.setSpeed(before, commitTimeInterval);
        sinkProcessInfo.setRest(sinkProcessInfo.getSkippedExcludeEventCount(), sinkProcessInfo.getSkippedCount());
        sinkProcessInfo.setTimestamp();
        long sourceCreateCount = inputCreateCount(createCountInfoPath + File.separator
                + CREATE_COUNT_INFO_NAME);
        while (sourceCreateCount != -1 && sourceCreateCount < sinkProcessInfo.getReplayedCount()
                + sinkProcessInfo.getSkippedCount() + sinkProcessInfo.getSkippedExcludeEventCount()) {
            sourceCreateCount = inputCreateCount(createCountInfoPath + File.separator
                    + CREATE_COUNT_INFO_NAME);
        }
        if (sourceCreateCount != -1) {
            sinkProcessInfo.setOverallPipe(sourceCreateCount);
        }
        return sinkProcessInfo.toString();
    }

    private String[] getCurrentGtid() {
        AtomicReference<String> currentGtidSet = new AtomicReference<>("");
        try {
            mysqlConnection.query(SHOW_MASTER_STATUS, rs -> {
                if (rs.next()) {
                    currentGtidSet.set(rs.getString(GTID));
                }
            });
        }
        catch (SQLException e) {
            LOGGER.error("SQL exception occurred when query the current event index.");
        }
        if ("".equals(currentGtidSet.get())) {
            return this.gtidSet;
        }
        return currentGtidSet.get().replaceAll("\\s*", "").split(",");
    }

    private long waitTimeInterval(boolean isSource) {
        long before;
        if (isSource) {
            before = sourceProcessInfo.getPollCount();
        }
        else {
            before = sinkProcessInfo.getReplayedCount();
        }
        try {
            Thread.sleep(commitTimeInterval * 1000L);
        }
        catch (InterruptedException exp) {
            LOGGER.warn("Interrupted exception occurred", exp);
        }
        return before;
    }

    private void executeOutPutThread(String dirPath) {
        threadPool.execute(() -> {
            if (!initFile(dirPath).exists()) {
                LOGGER.warn("Failed to output source create count, the sink overallPipe will always be 0.");
                threadPool.shutdown();
                return;
            }
            while (true) {
                try {
                    Thread.sleep(1000);
                }
                catch (InterruptedException exp) {
                    LOGGER.error("Interrupted exception occurred while thread sleeping", exp);
                }
                outputCreateCountInfo(dirPath + CREATE_COUNT_INFO_NAME, sourceProcessInfo
                        .getCreateCount() - sourceProcessInfo.getSkippedExcludeCount());
            }
        });
    }
}
