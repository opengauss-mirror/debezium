/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.process;

import com.alibaba.fastjson.JSON;
import io.debezium.config.SinkConnectorConfig;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.FileInputStream;
import java.io.IOException;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Description: BaseProcessCommitter
 *
 * @author wangzhengyuan
 * @since  2023-03-20
 */
public abstract class BaseProcessCommitter {
    private static final Logger LOGGER = LoggerFactory.getLogger(BaseProcessCommitter.class);

    /**
     * is append write
     */
    protected boolean isAppendWrite;

    /**
     * commit time interval
     */
    protected int commitTimeInterval;

    /**
     * create count information path
     */
    protected String createCountInfoPath;

    /**
     * file
     */
    protected final File file;
    private final String processFilePath;
    private final String filePrefix;
    private final int fileSizeLimit;
    private String fileFullPath;
    private File currentFile;
    private final DateTimeFormatter ofPattern = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH:mm:ss");

    /**
     * Constructor
     *
     * @param sourceConnectorConfig RelationalDatabaseConnectorConfig the sourceConnectorConfig
     * @param prefix String the prefix of file name
     */
    public BaseProcessCommitter(RelationalDatabaseConnectorConfig sourceConnectorConfig, String prefix) {
        this.processFilePath = sourceConnectorConfig.filePath();
        this.file = initFile(processFilePath);
        this.filePrefix = prefix;
        this.fileFullPath = initFileFullPath(file + File.separator + filePrefix);
        this.currentFile = new File(fileFullPath);
        this.commitTimeInterval = sourceConnectorConfig.commitTimeInterval();
        this.isAppendWrite = sourceConnectorConfig.appendWrite();
        this.fileSizeLimit = sourceConnectorConfig.fileSizeLimit();
        deleteRedundantFiles(sourceConnectorConfig.filePath(),
                sourceConnectorConfig.processFileCountLimit(), sourceConnectorConfig.processFileTimeLimit());
    }

    /**
     * Constructor
     *
     * @param sinkConnectorConfig SinkConnectorConfig the sinkConnectorConfig
     * @param prefix String the prefix
     */
    public BaseProcessCommitter(SinkConnectorConfig sinkConnectorConfig, String prefix) {
        this.processFilePath = sinkConnectorConfig.getSinkProcessFilePath();
        this.file = initFile(processFilePath);
        this.filePrefix = prefix;
        this.fileFullPath = initFileFullPath(file + File.separator + filePrefix);
        this.currentFile = new File(fileFullPath);
        this.createCountInfoPath = sinkConnectorConfig.getCreateCountInfoPath();
        this.isAppendWrite = sinkConnectorConfig.isAppend();
        this.fileSizeLimit = sinkConnectorConfig.getFileSizeLimit();
        this.commitTimeInterval = sinkConnectorConfig.getCommitTimeInterval();
        deleteRedundantFiles(sinkConnectorConfig.getSinkProcessFilePath(),
                sinkConnectorConfig.getProcessFileCountLimit(), sinkConnectorConfig.getProcessFileTimeLimit());
    }

    /**
     * Constructor
     *
     * @param failSqlPath String the fail sql path
     * @param fileSize int the file size
     */
    public BaseProcessCommitter(String failSqlPath, int fileSize) {
        this.processFilePath = failSqlPath;
        this.file = initFile(processFilePath);
        this.filePrefix = "fail-sqls-";
        this.fileFullPath = initFileFullPath(file + File.separator + filePrefix);
        this.currentFile = new File(fileFullPath);
        this.fileSizeLimit = fileSize;
    }

    /**
     * commit fail sqls
     *
     * @param failSql String the sql which replayed failed
     */
    public void commitFailSql(String failSql) {
        commit(failSql, true);
    }

    /**
     * Initialize the properties file of the class
     *
     * @param processFilePath String the file path
     * @return File the file
     */
    protected File initFile(String processFilePath) {
        File processFile = null;
        try {
            processFile = new File(processFilePath);
            if (! processFile.exists()) {
                Files.createDirectories(Paths.get(processFilePath));
            }
        } catch (IOException exp) {
            LOGGER.warn("Failed to create directors, please check file path.", exp);
        }
        return processFile;
    }

    /**
     * Initialize the properties fileFullPath of the class
     *
     * @param filePath String the file path
     * @return String the file full path
     */
    protected String initFileFullPath(String filePath) {
        if ("fail-sqls-".equals(this.filePrefix)) {
            return filePath + ofPattern.format(LocalDateTime.now()) + ".sql";
        }
        return filePath + ofPattern.format(LocalDateTime.now()) + ".txt";
    }

    /**
     * commit process or sqls replayed failed
     *
     * @param string String the string
     * @param isAppend Boolean the isAppend
     */
    protected void commit(String string, boolean isAppend) {
        if (isAppend && currentFile.exists() && currentFile.length() > fileSizeLimit * 1024L * 1024L) {
            fileFullPath = initFileFullPath(file + File.separator + filePrefix);
            currentFile = new File(fileFullPath);
        }
        if (file.exists()) {
            try(FileWriter fileWriter = new FileWriter(fileFullPath, isAppend)) {
                fileWriter.write(string + Utils.NL);
            } catch (IOException exp) {
                LOGGER.warn("IO exception occurred while committing message, process or fail sql will not be committed",
                        exp);
            }
        }
    }

    /**
     * input start event index or create count
     *
     * @param createCountFilePath String the createCountFilePath
     * @return Long the event index or create count
     */
    protected long inputCreateCount(String createCountFilePath) {
        File createCountFile = new File(createCountFilePath);
        if (!createCountFile.exists()) {
            return -1L;
        }
        Long fileLength;
        String content = "";
        byte[] fileContent;
        while ("".equals(content)) {
            fileLength = createCountFile.length();
            fileContent = new byte[fileLength.intValue()];
            try (FileInputStream in = new FileInputStream(createCountFile)) {
                in.read(fileContent);
            } catch (IOException exp) {
                LOGGER.warn("IO exception occurred while reading source create count,"
                        + " the overallPipe will always be 0", exp);
                return -1L;
            }
            content = new String(fileContent, StandardCharsets.UTF_8);
            content = content.trim();
        }
        String[] result = content.split(":");
        if (result.length < 2) {
            return -1L;
        }
        if (!"".equals(result[1])) {
            return Long.parseLong(result[1]);
        } else {
            return -1L;
        }
    }

    /**
     * delete redundant files
     *
     * @param processFilePath String the file path
     * @param processFileCountLimit Int the process file count limit
     * @param processFileTimeLimit Int the process file time limit
     */
    protected void deleteRedundantFiles(String processFilePath, int processFileCountLimit, int processFileTimeLimit) {
        File dir = new File(processFilePath);
        if (!dir.exists()) {
            return;
        }
        File[] files = dir.listFiles();
        File redundantFile;
        while (files.length > processFileCountLimit - 1) {
            redundantFile = files[0];
            for (int i = 1; i < files.length; i++) {
                if (files[i].lastModified() < redundantFile.lastModified()) {
                    redundantFile = files[i];
                }
            }
            redundantFile.delete();
            files = dir.listFiles();
        }

        for (File file : files) {
            if (file.lastModified() < System.currentTimeMillis() - processFileTimeLimit * 3600 * 1000L) {
                file.delete();
            }
        }
    }

    /**
     * output create count information
     *
     * @param filePath the file path that the tool output
     * @param number the event index of incremental migration tool or
     *              the effective create count of reverse migration tool
     */
    protected void outputCreateCountInfo(String filePath, long number) {
        try (FileWriter fileWriter = new FileWriter(filePath)) {
            fileWriter.write(System.currentTimeMillis() + ":" + number + "");
        } catch (IOException exp) {
            LOGGER.warn("IO exception occurred while output source create count,"
                    + " the overallPipe will always be 0", exp);
        }
    }

    /**
     * commit source process information
     */
    public void commitSourceProcessInfo() {
        while (true) {
            String json = JSON.toJSONString(statSourceProcessInfo());
            commit(json, isAppendWrite);
        }
    }

    /**
     * commit sink process information
     */
    public void commitSinkProcessInfo() {
        while (true) {
            String json = JSON.toJSONString(statSinkProcessInfo());
            commit(json, isAppendWrite);
        }
    }

    /**
     * statSourceProcessInfo
     *
     * @return BaseSourceProcessInfo the source process information
     */
    protected abstract BaseSourceProcessInfo statSourceProcessInfo();

    /**
     * statSinkProcessInfo
     *
     * @return BaseSinkProcessInfo the sink process information
     */
    protected abstract BaseSinkProcessInfo statSinkProcessInfo();
}
