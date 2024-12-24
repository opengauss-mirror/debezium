/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.process;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.SinkConnectorConfig;
import io.debezium.enums.ErrorCode;
import io.debezium.relational.RelationalDatabaseConnectorConfig;

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
     * fileFullPath
     */
    protected String fileFullPath;

    /**
     * currentFile
     */
    protected File currentFile;

    /**
     * file
     */
    protected final File file;
    private final String processFilePath;
    private final String filePrefix;
    private final int fileSizeLimit;
    private final DateTimeFormatter ofPattern = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH:mm:ss");

    /**
     * Constructor
     *
     * @param processFilePath processFilePath
     * @param prefix prefix
     * @param commitTimeInterval commitTimeInterval
     * @param fileSizeLimit fileSizeLimit
     */
    public BaseProcessCommitter(String processFilePath, String prefix, int commitTimeInterval, int fileSizeLimit) {
        this.processFilePath = processFilePath;
        this.file = initFile(this.processFilePath);
        this.filePrefix = prefix;
        this.commitTimeInterval = commitTimeInterval;
        this.fileSizeLimit = fileSizeLimit;
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
     * Get
     *
     * @return file
     */
    public File getFile() {
        return file;
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
            if (!processFile.exists()) {
                Files.createDirectories(Paths.get(processFilePath));
            }
        }
        catch (IOException exp) {
            LOGGER.warn("{}Failed to create directors, please check file path.", ErrorCode.PROGRESS_COMMIT_EXCEPTION,
                exp);
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
            try (FileWriter fileWriter = new FileWriter(fileFullPath, isAppend)) {
                fileWriter.write(string + Utils.NL);
            }
            catch (IOException exp) {
                LOGGER.warn(
                    "{}IO exception occurred while committing message, process or fail sql will not be committed",
                    ErrorCode.PROGRESS_COMMIT_EXCEPTION, exp);
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
            }
            catch (IOException exp) {
                LOGGER.warn("{}IO exception occurred while reading source create count,"
                        + " the overallPipe will always be 0", ErrorCode.PROGRESS_COMMIT_EXCEPTION, exp);
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
        }
        else {
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
        File[] files = dir.listFiles((directory, name) -> name.startsWith(filePrefix));
        File redundantFile;
        while (files.length > processFileCountLimit - 1) {
            redundantFile = files[0];
            for (int i = 1; i < files.length; i++) {
                if (files[i].lastModified() < redundantFile.lastModified()) {
                    redundantFile = files[i];
                }
            }
            redundantFile.delete();
            files = dir.listFiles((directory, name) -> name.startsWith(filePrefix));
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
        }
        catch (IOException exp) {
            LOGGER.warn("{}IO exception occurred while output source create count,"
                    + " the overallPipe will always be 0", ErrorCode.PROGRESS_COMMIT_EXCEPTION, exp);
        }
    }

    /**
     * commit source process information
     */
    public void commitSourceProcessInfo() {
        while (true) {
            commit(statSourceProcessInfo(), isAppendWrite);
        }
    }

    /**
     * commit sink process information
     */
    public void commitSinkProcessInfo() {
        while (true) {
            commit(statSinkProcessInfo(), isAppendWrite);
        }
    }

    /**
     * statSourceProcessInfo
     *
     * @return BaseSourceProcessInfo the source process information
     */
    protected abstract String statSourceProcessInfo();

    /**
     * statSinkProcessInfo
     *
     * @return BaseSinkProcessInfo the sink process information
     */
    protected abstract String statSinkProcessInfo();
}
