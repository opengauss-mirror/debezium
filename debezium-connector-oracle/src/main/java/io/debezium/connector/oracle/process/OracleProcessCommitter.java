/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.process;

import java.io.File;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.sink.task.OracleSinkConnectorConfig;
import io.debezium.connector.process.BaseProcessCommitter;
import io.debezium.connector.process.BaseSourceProcessInfo;

/**
 * Description: OracleProcessCommitter
 *
 * @author gbase
 * @since  2023-07-28
 */
public class OracleProcessCommitter extends BaseProcessCommitter {
    private static final Logger LOGGER = LoggerFactory.getLogger(OracleProcessCommitter.class);
    private static final String FORWARD_SOURCE_PROCESS_PREFIX = "forward-source-process-";
    private static final String FORWARD_SINK_PROCESS_PREFIX = "forward-sink-process-";
    private static final String CREATE_COUNT_INFO_NAME = "source-create-count.txt";

    private final ThreadPoolExecutor threadPool = new ThreadPoolExecutor(1, 1, 100,
            TimeUnit.SECONDS, new LinkedBlockingQueue<>(1));
    private BaseSourceProcessInfo sourceProcessInfo;
    private OracleSinkProcessInfo sinkProcessInfo;

    private boolean isParallelBasedTransaction;

    /**
     * Constructor
     *
     * @param processFilePath processFilePath
     * @param prefix prefix
     * @param commitTimeInterval commitTimeInterval
     * @param fileSizeLimit fileSizeLimit
     */
    public OracleProcessCommitter(String processFilePath, String prefix, int commitTimeInterval, int fileSizeLimit) {
        super(processFilePath, prefix, commitTimeInterval, fileSizeLimit);
    }

    /**
     * Constructor
     *
     * @param connectorConfig OracleConnectorConfig the connectorConfig
     */
    public OracleProcessCommitter(OracleConnectorConfig connectorConfig) {
        this(connectorConfig.filePath(), FORWARD_SOURCE_PROCESS_PREFIX,
                connectorConfig.commitTimeInterval(), connectorConfig.fileSizeLimit());
        this.fileFullPath = initFileFullPath(file + File.separator + FORWARD_SOURCE_PROCESS_PREFIX);
        this.currentFile = new File(fileFullPath);
        this.isAppendWrite = connectorConfig.appendWrite();
        deleteRedundantFiles(connectorConfig.filePath(),
                connectorConfig.processFileCountLimit(), connectorConfig.processFileTimeLimit());
        this.isParallelBasedTransaction = connectorConfig.shouldProvideTransactionMetadata();
        if (isParallelBasedTransaction) {
            this.sourceProcessInfo = BaseSourceProcessInfo.TRANSACTION_SOURCE_PROCESS_INFO;
        } else {
            this.sourceProcessInfo = BaseSourceProcessInfo.TABLE_SOURCE_PROCESS_INFO;
        }
        outputCreateCountThread(connectorConfig.createCountInfoPath() + File.separator);
    }

    /**
     * Constructor
     *
     * @param connectorConfig OracleSinkConnectorConfig the connectorConfig
     */
    public OracleProcessCommitter(OracleSinkConnectorConfig connectorConfig) {
        this(connectorConfig.getSinkProcessFilePath(), FORWARD_SINK_PROCESS_PREFIX,
                connectorConfig.getCommitTimeInterval(), connectorConfig.getFileSizeLimit());
        this.fileFullPath = initFileFullPath(file + File.separator + FORWARD_SINK_PROCESS_PREFIX);
        this.currentFile = new File(fileFullPath);
        this.isAppendWrite = connectorConfig.isAppend();
        this.createCountInfoPath = connectorConfig.getCreateCountInfoPath();
        this.isParallelBasedTransaction = connectorConfig.isParallelBasedTransaction;
        sinkProcessInfo = OracleSinkProcessInfo.SINK_PROCESS_INFO;
        deleteRedundantFiles(connectorConfig.getSinkProcessFilePath(),
                connectorConfig.getProcessFileCountLimit(), connectorConfig.getProcessFileTimeLimit());
    }

    /**
     * Constructor
     *
     * @param failSqlPath String the fail sql path
     * @param fileSize int the file size
     */
    public OracleProcessCommitter(String failSqlPath, int fileSize) {
        super(failSqlPath, fileSize);
    }

    /**
     * statSourceProcessInfo
     *
     * @return OracleSourceProcessInfo the oracleSourceProcessInfo
     */
    protected String statSourceProcessInfo() {
        long before = waitTimeInterval(true);
        sourceProcessInfo.setSpeed(before, commitTimeInterval);
        sourceProcessInfo.setRest();
        sourceProcessInfo.setTimestamp();
        return sourceProcessInfo.toString();
    }

    /**
     * statSinkProcessInfo
     *
     * @return OracleSourceProcessInfo the oracleSourceProcessInfo
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

    private long waitTimeInterval(boolean isSource) {
        long before;
        if (isSource) {
            before = sourceProcessInfo.getPollCount();
        } else {
            before = sinkProcessInfo.getReplayedCount();
        }
        try {
            TimeUnit.SECONDS.sleep(commitTimeInterval);
        } catch (InterruptedException exp) {
            LOGGER.warn("Interrupted exception occurred", exp);
        }
        return before;
    }

    private void outputCreateCountThread(String filePath) {
        threadPool.execute(() -> {
            if (!initFile(filePath).exists()) {
                LOGGER.warn("Failed to output source create count, the sink overallPipe will always be 0.");
                threadPool.shutdown();
                return;
            }
            while (true) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException exp) {
                    LOGGER.error("Interrupted exception occurred while thread sleeping", exp);
                }
                outputCreateCountInfo(filePath + CREATE_COUNT_INFO_NAME, sourceProcessInfo.getCreateCount()
                        - sourceProcessInfo.getSkippedExcludeCount());
            }
        });
    }
}
