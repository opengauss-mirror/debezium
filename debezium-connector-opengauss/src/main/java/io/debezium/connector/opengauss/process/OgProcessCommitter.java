/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.opengauss.process;

import com.alibaba.fastjson.JSON;
import io.debezium.connector.opengauss.OpengaussConnectorConfig;
import io.debezium.connector.opengauss.sink.task.OpengaussSinkConnectorConfig;
import io.debezium.connector.process.BaseProcessCommitter;
import io.debezium.connector.process.BaseSourceProcessInfo;
import io.debezium.enums.ErrorCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Description: OgProcessCommitter
 *
 * @author wangzhengyuan
 * @since 2023-03-20
 */
public class OgProcessCommitter extends BaseProcessCommitter {
    /**
     *  full progress report file suffix
     */
    public static final String REVERSE_FULL_PROCESS_SUFFIX = "full.txt";
    private static final Logger LOGGER = LoggerFactory.getLogger(OgProcessCommitter.class);
    private static final String REVERSE_SOURCE_PROCESS_PREFIX = "reverse-source-process-";
    private static final String REVERSE_SINK_PROCESS_PREFIX = "reverse-sink-process-";
    private static final String CREATE_COUNT_INFO_NAME = "source-create-count.txt";

    private final ThreadPoolExecutor threadPool = new ThreadPoolExecutor(1, 1, 100,
            TimeUnit.SECONDS, new LinkedBlockingQueue<>(1));
    private BaseSourceProcessInfo sourceProcessInfo = BaseSourceProcessInfo.TABLE_SOURCE_PROCESS_INFO;
    private OgSinkProcessInfo sinkProcessInfo;
    private String sharePath;

    /**
     * Constructor
     *
     * @param processFilePath processFilePath
     * @param prefix prefix
     * @param commitTimeInterval commitTimeInterval
     * @param fileSizeLimit fileSizeLimit
     */
    public OgProcessCommitter(String processFilePath, String prefix, int commitTimeInterval, int fileSizeLimit) {
        super(processFilePath, prefix, commitTimeInterval, fileSizeLimit);
    }

    /**
     * Constructor
     *
     * @param connectorConfig OpengaussConnectorConfig the connectorConfig
     */
    public OgProcessCommitter(OpengaussConnectorConfig connectorConfig) {
        this(connectorConfig.filePath(), REVERSE_SOURCE_PROCESS_PREFIX,
                connectorConfig.commitTimeInterval(), connectorConfig.fileSizeLimit());
        this.fileFullPath = initFileFullPath(file + File.separator + REVERSE_SOURCE_PROCESS_PREFIX);
        this.currentFile = new File(fileFullPath);
        this.isAppendWrite = connectorConfig.appendWrite();
        deleteRedundantFiles(connectorConfig.filePath(),
                connectorConfig.processFileCountLimit(), connectorConfig.processFileTimeLimit());
        outputCreateCountThread(connectorConfig.createCountInfoPath() + File.separator);
    }

    /**
     * Constructor
     *
     * @param connectorConfig OpengaussSinkConnectorConfig the connectorConfig
     */
    public OgProcessCommitter(OpengaussSinkConnectorConfig connectorConfig) {
        this(connectorConfig.getSinkProcessFilePath(), REVERSE_SOURCE_PROCESS_PREFIX,
                connectorConfig.getCommitTimeInterval(), connectorConfig.getFileSizeLimit());
        this.fileFullPath = initFileFullPath(file + File.separator + REVERSE_SINK_PROCESS_PREFIX);
        this.currentFile = new File(fileFullPath);
        this.isAppendWrite = connectorConfig.isAppend();
        this.createCountInfoPath = connectorConfig.getCreateCountInfoPath();
        deleteRedundantFiles(connectorConfig.getSinkProcessFilePath(),
                connectorConfig.getProcessFileCountLimit(), connectorConfig.getProcessFileTimeLimit());
    }

    /**
     * Constructor
     *
     * @param connectorConfig OpengaussConnectorConfig the connectorConfig
     * @param suffix file suffix
     */
    public OgProcessCommitter(OpengaussConnectorConfig connectorConfig, String suffix) {
        this(connectorConfig.filePath(), REVERSE_SOURCE_PROCESS_PREFIX,
                connectorConfig.commitTimeInterval(), connectorConfig.fileSizeLimit());
        this.fileFullPath = new File(connectorConfig.createCountInfoPath()) + File.separator
                + REVERSE_SOURCE_PROCESS_PREFIX + suffix;
        this.currentFile = new File(fileFullPath);
    }

    /**
     * Constructor
     *
     * @param connectorConfig OpengaussSinkConnectorConfig the connectorConfig
     * @param suffix file suffix
     */
    public OgProcessCommitter(OpengaussSinkConnectorConfig connectorConfig, String suffix) {
        this(connectorConfig.getSinkProcessFilePath(), REVERSE_SOURCE_PROCESS_PREFIX,
                connectorConfig.getCommitTimeInterval(), connectorConfig.getFileSizeLimit());
        this.fileFullPath = file + File.separator + REVERSE_SINK_PROCESS_PREFIX + suffix;
        this.currentFile = new File(fileFullPath);
        this.sharePath = connectorConfig.getCreateCountInfoPath();
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
     * Obtain all information about the source end
     *
     * @return OgFullSourceProcessInfo
     */
    public OgFullSourceProcessInfo getSourceFileJson() {
        String fileFullPath = getSourceFullFilePath();
        try (Stream<String> stream = Files.lines(Paths.get(fileFullPath));) {
            List<String> list = stream.collect(Collectors.toList());
            if (list.size() == 1) {
                return JSON.parseObject(list.get(0), OgFullSourceProcessInfo.class);
            }
        } catch (IOException e) {
            LOGGER.error("{}Progress report get source file failure.", ErrorCode.IO_EXCEPTION, e);
        }
        return new OgFullSourceProcessInfo();
    }

    /**
     * Whether the file contains information
     *
     * @return boolean
     */
    public boolean hasMessage() {
        String fileFullPath = getSourceFullFilePath();
        File file = new File(fileFullPath);
        if (!file.exists()) {
            return false;
        }
        return file.length() > 0;
    }

    /**
     * commit source Table process information
     */
    public void commitSourceTableProcessInfo(OgFullSourceProcessInfo ogFullSourceProcessInfo) {
        String json = JSON.toJSONString(ogFullSourceProcessInfo);
        commit(json, false);
    }

    /**
     * commit sink Table process information
     */
    public void commitSinkTableProcessInfo(OgFullSinkProcessInfo ogFullSinkProcessInfo) {
        String json = JSON.toJSONString(ogFullSinkProcessInfo);
        commit(json, false);
    }

    /**
     * statSourceProcessInfo
     *
     * @return OgSourceProcessInfo the opengauss source process information
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
     * @return OgSinkProcessInfo the opengauss sink process information
     */
    protected String statSinkProcessInfo() {
        long before = waitTimeInterval(false);
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
        return sinkProcessInfo.toString();
    }

    private String getSourceFullFilePath() {
        File file = new File(this.sharePath);
        return file + File.separator + REVERSE_SOURCE_PROCESS_PREFIX + REVERSE_FULL_PROCESS_SUFFIX;
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
                    LOGGER.error("{}Interrupted exception occurred while thread sleeping",
                        ErrorCode.THREAD_INTERRUPTED_EXCEPTION, exp);
                }
                outputCreateCountInfo(filePath + CREATE_COUNT_INFO_NAME, sourceProcessInfo.getCreateCount()
                        - sourceProcessInfo.getSkippedExcludeCount());
            }
        });
    }

    private long waitTimeInterval(boolean isSource) {
        long before;
        if (isSource) {
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