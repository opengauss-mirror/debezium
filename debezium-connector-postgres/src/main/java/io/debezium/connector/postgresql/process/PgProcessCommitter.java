/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package io.debezium.connector.postgresql.process;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;

import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.sink.task.PostgresSinkConnectorConfig;
import io.debezium.connector.process.BaseProcessCommitter;
import io.debezium.connector.process.BaseSourceProcessInfo;

/**
 * Description: PgProcessCommitter
 *
 * @author jianghongbo
 * @since 2025-02-05
 */
public class PgProcessCommitter extends BaseProcessCommitter {
    /**
     *  full progress report file suffix
     */
    public static final String FULL_PROCESS_SUFFIX = "full.txt";

    /**
     *  txt file suffix
     */
    public static final String PROCESS_SUFFIX = ".txt";
    private static final Logger LOGGER = LoggerFactory.getLogger(PgProcessCommitter.class);
    private static final String FORWARD_SOURCE_PROCESS_PREFIX = "forward-source-process-";
    private static final String FORWARD_SINK_PROCESS_PREFIX = "forward-sink-process-";
    private static final String CREATE_COUNT_INFO_NAME = "start-event-count.txt";

    private final ThreadPoolExecutor threadPool = new ThreadPoolExecutor(1, 1, 100,
            TimeUnit.SECONDS, new LinkedBlockingQueue<>(1));
    private BaseSourceProcessInfo sourceProcessInfo = BaseSourceProcessInfo.TABLE_SOURCE_PROCESS_INFO;
    private PgSinkProcessInfo sinkProcessInfo;
    private String sharePath;

    /**
     * Constructor
     *
     * @param processFilePath processFilePath
     * @param prefix prefix
     * @param commitTimeInterval commitTimeInterval
     * @param fileSizeLimit fileSizeLimit
     */
    public PgProcessCommitter(String processFilePath, String prefix, int commitTimeInterval, int fileSizeLimit) {
        super(processFilePath, prefix, commitTimeInterval, fileSizeLimit);
    }

    /**
     * Constructor
     *
     * @param connectorConfig PostgresConnectorConfig the connectorConfig
     */
    public PgProcessCommitter(PostgresConnectorConfig connectorConfig) {
        this(connectorConfig.filePath(), FORWARD_SOURCE_PROCESS_PREFIX,
                connectorConfig.commitTimeInterval(), connectorConfig.fileSizeLimit());
        this.fileFullPath = initFileFullPath(file + File.separator + FORWARD_SOURCE_PROCESS_PREFIX);
        this.currentFile = new File(fileFullPath);
        this.isAppendWrite = connectorConfig.appendWrite();
        deleteRedundantFiles(connectorConfig.filePath(),
                connectorConfig.processFileCountLimit(), connectorConfig.processFileTimeLimit());
        outputCreateCountThread(connectorConfig.createCountInfoPath() + File.separator);
    }

    /**
     * Constructor
     *
     * @param connectorConfig PostgresSinkConnectorConfig the connectorConfig
     */
    public PgProcessCommitter(PostgresSinkConnectorConfig connectorConfig) {
        this(connectorConfig.getSinkProcessFilePath(), FORWARD_SINK_PROCESS_PREFIX,
                connectorConfig.getCommitTimeInterval(), connectorConfig.getFileSizeLimit());
        this.fileFullPath = initFileFullPath(file + File.separator + FORWARD_SINK_PROCESS_PREFIX);
        this.currentFile = new File(fileFullPath);
        this.isAppendWrite = connectorConfig.isAppend();
        this.createCountInfoPath = connectorConfig.getCreateCountInfoPath();
        deleteRedundantFiles(connectorConfig.getSinkProcessFilePath(),
                connectorConfig.getProcessFileCountLimit(), connectorConfig.getProcessFileTimeLimit());
    }

    /**
     * Constructor
     *
     * @param connectorConfig PostgresConnectorConfig the connectorConfig
     * @param suffix file suffix
     */
    public PgProcessCommitter(PostgresConnectorConfig connectorConfig, String suffix) {
        this(connectorConfig.filePath(), FORWARD_SOURCE_PROCESS_PREFIX,
                connectorConfig.commitTimeInterval(), connectorConfig.fileSizeLimit());
        this.fileFullPath = new File(connectorConfig.createCountInfoPath()) + File.separator
                + FORWARD_SOURCE_PROCESS_PREFIX + suffix;
        this.currentFile = new File(fileFullPath);
    }

    /**
     * Constructor
     *
     * @param connectorConfig PostgresSinkConnectorConfig the connectorConfig
     * @param suffix file suffix
     */
    public PgProcessCommitter(PostgresSinkConnectorConfig connectorConfig, String suffix) {
        this(connectorConfig.getSinkProcessFilePath(), FORWARD_SINK_PROCESS_PREFIX,
                connectorConfig.getCommitTimeInterval(), connectorConfig.getFileSizeLimit());
        this.fileFullPath = file + File.separator + FORWARD_SINK_PROCESS_PREFIX + suffix;
        this.currentFile = new File(fileFullPath);
        this.sharePath = connectorConfig.getCreateCountInfoPath();
    }

    /**
     * Constructor
     *
     * @param failSqlPath String the fail sql path
     * @param fileSize int the file size
     */
    public PgProcessCommitter(String failSqlPath, int fileSize) {
        super(failSqlPath, fileSize);
    }

    /**
     * Obtain all information about the source end
     *
     * @return PgFullSourceProcessInfo
     */
    public PgFullSourceProcessInfo getSourceFileJson() {
        String fileFullPath = getSourceFullFilePath();
        try (Stream<String> stream = Files.lines(Paths.get(fileFullPath));) {
            List<String> list = stream.collect(Collectors.toList());
            if (list.size() == 1) {
                return JSON.parseObject(list.get(0), PgFullSourceProcessInfo.class);
            }
        } catch (IOException e) {
            LOGGER.error("Progress report get source file failure.", e);
        }
        return new PgFullSourceProcessInfo();
    }

    /**
     * commit sink Table process information
     *
     * @param pgOfflineSinkProcessInfo PgOfflineSinkProcessInfo
     */
    public void commitSinkTableProcessInfo(PgOfflineSinkProcessInfo pgOfflineSinkProcessInfo) {
        String json = JSON.toJSONString(pgOfflineSinkProcessInfo);
        commit(json, false);
    }

    /**
     * get all tables from source file
     *
     * @return List<String> table list
     */
    public List<String> getSourceTableList() {
        List<String> tableList = new ArrayList<>();
        try (Stream<String> stream = Files.lines(Paths.get(getSourceFullFilePath()))) {
            List<String> lst = stream.collect(Collectors.toList());
            if (lst.size() != 1) {
                throw new IllegalStateException("Expected exactly one line in the source file");
            }
            tableList = JSON.parseArray(lst.get(0), String.class);
        } catch (IOException e) {
            LOGGER.error("read source table list error", e);
        }
        return tableList;
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
     *
     * @param pgFullSourceProcessInfo PgFullSourceProcessInfo
     */
    public void commitSourceTableProcessInfo(PgFullSourceProcessInfo pgFullSourceProcessInfo) {
        String json = JSON.toJSONString(pgFullSourceProcessInfo);
        commit(json, false);
    }

    /**
     * commit source Table list
     *
     * @param tableList List<String> tableName list
     */
    public void commitSourceTableList(List<String> tableList) {
        commit(JSON.toJSONString(tableList), false);
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
        sinkProcessInfo.setOverallPipe(sourceCreateCount);
        return sinkProcessInfo.toString();
    }

    private String getSourceFullFilePath() {
        File file = new File(this.sharePath);
        return file + File.separator + FORWARD_SOURCE_PROCESS_PREFIX + FULL_PROCESS_SUFFIX;
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
            before = sourceProcessInfo.getPollCount();
        } else {
            sinkProcessInfo = PgSinkProcessInfo.SINK_PROCESS_INFO;
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
