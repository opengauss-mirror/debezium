/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.opengauss.process;

import io.debezium.connector.opengauss.OpengaussConnectorConfig;
import io.debezium.connector.opengauss.sink.task.OpengaussSinkConnectorConfig;

import io.debezium.connector.process.BaseProcessCommitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Description: OgProcessCommitter
 *
 * @author wangzhengyuan
 * @since 2023-03-20
 */
public class OgProcessCommitter extends BaseProcessCommitter {
    private static final Logger LOGGER = LoggerFactory.getLogger(OgProcessCommitter.class);
    private static final String REVERSE_SOURCE_PROCESS_PREFIX = "reverse-source-process-";
    private static final String REVERSE_SINK_PROCESS_PREFIX = "reverse-sink-process-";
    private static final String CREATE_COUNT_INFO_NAME = "source-create-count.txt";

    private final ThreadPoolExecutor threadPool = new ThreadPoolExecutor(1, 1, 100,
            TimeUnit.SECONDS, new LinkedBlockingQueue<>(1));
    private OgSourceProcessInfo sourceProcessInfo;
    private OgSinkProcessInfo sinkProcessInfo;

    /**
     * Constructor
     *
     * @param connectorConfig OpengaussConnectorConfig the connectorConfig
     */
    public OgProcessCommitter(OpengaussConnectorConfig connectorConfig) {
        super(connectorConfig, REVERSE_SOURCE_PROCESS_PREFIX);
        outputCreateCountThread(connectorConfig.createCountInfoPath() + File.separator);
    }

    /**
     * Constructor
     *
     * @param connectorConfig OpengaussSinkConnectorConfig the connectorConfig
     */
    public OgProcessCommitter(OpengaussSinkConnectorConfig connectorConfig) {
        super(connectorConfig, REVERSE_SINK_PROCESS_PREFIX);
    }

    /**
     * Constructor
     *
     * @param failSqlPath String the fail sql path
     * @param fileSize int the file size
     */
    public OgProcessCommitter(String failSqlPath, int fileSize) {
        super(failSqlPath, fileSize);
    }

    /**
     * statSourceProcessInfo
     *
     * @return OgSourceProcessInfo the opengauss source process information
     */
    protected OgSourceProcessInfo statSourceProcessInfo() {
        long before = waitTimeInterval(true);
        sourceProcessInfo = OgSourceProcessInfo.SOURCE_PROCESS_INFO;
        sourceProcessInfo.setSpeed(before, commitTimeInterval);
        sourceProcessInfo.setRest(sourceProcessInfo.getSkippedExcludeCount());
        sourceProcessInfo.setTimestamp();
        return sourceProcessInfo;
    }

    /**
     * statSinkProcessInfo
     *
     * @return OgSinkProcessInfo the opengauss sink process information
     */
    protected OgSinkProcessInfo statSinkProcessInfo() {
        long before = waitTimeInterval(false);
        sinkProcessInfo = OgSinkProcessInfo.SINK_PROCESS_INFO;
        sinkProcessInfo.setSpeed(before, commitTimeInterval);
        sinkProcessInfo.setRest(0, 0);
        sinkProcessInfo.setTimestamp();
        long sourceCreateCount = inputCreateCount(createCountInfoPath + File.separator
                + CREATE_COUNT_INFO_NAME);
        while (sourceCreateCount != -1 && sourceCreateCount < sinkProcessInfo.getReplayedCount()) {
            sourceCreateCount = inputCreateCount(createCountInfoPath + File.separator
                    + CREATE_COUNT_INFO_NAME);
        }
        if (sourceCreateCount != -1) {
            sinkProcessInfo.setOverallPipe(sourceCreateCount);
        }
        return sinkProcessInfo;
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

    private long waitTimeInterval(boolean isSource) {
        long before;
        if (isSource) {
            sourceProcessInfo = OgSourceProcessInfo.SOURCE_PROCESS_INFO;
            before = sourceProcessInfo.getPollCount();
        } else {
            sinkProcessInfo = OgSinkProcessInfo.SINK_PROCESS_INFO;
            before = sinkProcessInfo.getReplayedCount();
        }
        try {
            Thread.sleep(commitTimeInterval * 1000L);
        } catch (InterruptedException exp) {
            LOGGER.warn("Interrupted exception occurred", exp);
        }
        return before;
    }
}