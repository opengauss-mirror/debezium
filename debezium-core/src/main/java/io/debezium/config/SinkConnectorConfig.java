/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.config;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Map;

/**
 * Description: OpengaussSinkConnectorConfig class
 *
 * @author wangzhengyuan
 * @since 2023-04-28
 */
public class SinkConnectorConfig extends AbstractConfig {
    /**
     * Topics
     */
    public static final String TOPICS = "topics";

    /**
     * Schema mappings
     */
    public static final String SCHEMA_MAPPINGS = "schema.mappings";

    /**
     * commit process while running
     */
    public static final String COMMIT_PROCESS_WHILE_RUNNING = "commit.process.while.running";

    /**
     * sink process file path
     */
    public static final String PROCESS_FILE_PATH = "sink.process.file.path";

    /**
     * fail sql file path
     */
    public static final String FAIL_SQL_PATH = "fail.sql.path";

    /**
     * commit time interval
     */
    public static final String COMMIT_TIME_INTERVAL = "commit.time.interval";

    /**
     * create count information path
     */

    public static final String CREATE_COUNT_INFO_PATH = "create.count.info.path";

    /**
     * process file count limit
     */
    public static final String PROCESS_FILE_COUNT_LIMIT = "process.file.count.limit";

    /**
     * process file time limit
     */
    public static final String PROCESS_FILE_TIME_LIMIT = "process.file.time.limit";

    /**
     * append write
     */
    public static final String APPEND_WRITE = "append.write";

    /**
     * file size limit
     */
    public static final String FILE_SIZE_LIMIT = "file.size.limit";

    /**
     * CONFIG_DEF
     */
    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(TOPICS, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "topics")
            .define(SCHEMA_MAPPINGS, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "schema mappings")
            .define(COMMIT_PROCESS_WHILE_RUNNING, ConfigDef.Type.STRING, "false",
                    ConfigDef.Importance.HIGH, "commit process while running")
            .define(PROCESS_FILE_PATH, ConfigDef.Type.STRING, getCurrentPluginPath() + "sink" + File.separator,
                    ConfigDef.Importance.HIGH, "sink process file path")
            .define(FAIL_SQL_PATH, ConfigDef.Type.STRING, getCurrentPluginPath() + "fail-sqls" + File.separator,
                    ConfigDef.Importance.HIGH, "fail sql file path")
            .define(COMMIT_TIME_INTERVAL, ConfigDef.Type.STRING, "1", ConfigDef.Importance.HIGH, "commit time interval")
            .define(CREATE_COUNT_INFO_PATH, ConfigDef.Type.STRING, getCurrentPluginPath(),
                    ConfigDef.Importance.HIGH, "create count information path")
            .define(PROCESS_FILE_COUNT_LIMIT, ConfigDef.Type.STRING, "10",
                    ConfigDef.Importance.HIGH, "process file count limit")
            .define(PROCESS_FILE_TIME_LIMIT, ConfigDef.Type.STRING, "168",
                    ConfigDef.Importance.HIGH, "process file time limit")
            .define(APPEND_WRITE, ConfigDef.Type.STRING, "false", ConfigDef.Importance.HIGH, "append write")
            .define(FILE_SIZE_LIMIT, ConfigDef.Type.STRING, "10", ConfigDef.Importance.HIGH, "file size limit");
    private static final Logger LOGGER = LoggerFactory.getLogger(SinkConnectorConfig.class);

    /**
     * Topics
     */
    public final String topics;

    /**
     * Schema mapping
     */
    public final String schemaMappings;
    private boolean isCommitProcess = false;
    private String sinkProcessFilePath = getCurrentPluginPath() + "sink" + File.separator;
    private String failSqlPath = getCurrentPluginPath() + "fail-sqls" + File.separator;
    private int commitTimeInterval = 1;
    private String createCountInfoPath = getCurrentPluginPath();
    private int processFileCountLimit = 10;
    private int processFileTimeLimit = 168;
    private boolean isAppendWrite = false;
    private int fileSizeLimit = 10;

    /**
     * Constructor
     *
     * @param configDef ConfigDef the configDef
     * @param originals Map<?, ?> the originals
     */
    public SinkConnectorConfig(ConfigDef configDef, Map<?, ?> originals) {
        super(configDef, originals, false);
        this.topics = getString(TOPICS);
        this.schemaMappings = getString(SCHEMA_MAPPINGS);
        if (isCommitProcess()) {
            rectifyParameter();
        }
        rectifyFailSqlPara();
    }

    /**
     * get current plugin path
     *
     * @return String the plugin path
     */
    public static String getCurrentPluginPath() {
        String path = SinkConnectorConfig.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        StringBuilder sb = new StringBuilder();
        String[] paths = path.split(File.separator);
        for (int i = 0; i < paths.length - 2; i++) {
            sb.append(paths[i]).append(File.separator);
        }
        return sb.toString();
    }

    /**
     * get config def
     *
     * @return ConfigDef the CONFIG_DEF
     */
    public static ConfigDef getConfigDef() {
        return CONFIG_DEF;
    }

    /**
     * is commit process
     *
     * @return Boolean the isCommitProcess
     */
    public Boolean isCommitProcess() {
        if (isBooleanValid(COMMIT_PROCESS_WHILE_RUNNING)) {
            isCommitProcess = Boolean.parseBoolean(getString(COMMIT_PROCESS_WHILE_RUNNING));
        }
        return isCommitProcess;
    }

    /**
     * is append write
     *
     * @return Boolean the isAppendWrite
     */
    public Boolean isAppend() {
        return isAppendWrite;
    }

    /**
     * is fail sql file path valid
     *
     * @return Boolean the isFailSqlFilePathValid
     */
    public Boolean isFailSqlFilePathValid() {
        if ("".equals(failSqlPath) || failSqlPath.charAt(0) != File.separatorChar) {
            LOGGER.warn("The parameter " + FAIL_SQL_PATH + " is invalid, it must be absolute path,"
                    + " fail sqls won't be committed.");
            return false;
        }
        return true;
    }

    /**
     * Gets
     *
     * @return String the sink process file path
     */
    public String getSinkProcessFilePath() {
        return sinkProcessFilePath;
    }

    /**
     * Gets
     *
     * @return String the fail sql file path
     */
    public String getFailSqlPath() {
        return failSqlPath;
    }

    /**
     * Gets
     *
     * @return String the create count information path
     */
    public String getCreateCountInfoPath() {
        return createCountInfoPath;
    }

    /**
     * Gets
     *
     * @return Integer the commit time interval
     */
    public Integer getCommitTimeInterval() {
        return commitTimeInterval;
    }

    /**
     * Gets
     *
     * @return Integer the process file count limit
     */
    public Integer getProcessFileCountLimit() {
        return processFileCountLimit;
    }

    /**
     * Gets
     *
     * @return Integer the file time limit
     */
    public Integer getProcessFileTimeLimit() {
        return processFileTimeLimit;
    }

    /**
     * Gets
     *
     * @return Integer the file size limit
     */
    public Integer getFileSizeLimit() {
        return fileSizeLimit;
    }

    private boolean isFilePathValid(String parameterName, String defaultValue) {
        String value = getString(parameterName);
        if ("".equals(value) || value.charAt(0) != File.separatorChar) {
            LOGGER.warn("The parameter " + parameterName + " is invalid, it must be absolute path,"
                    + " will adopt it's default value: " + defaultValue);
            return false;
        }
        return true;
    }

    private Boolean isNumberValid(String parameterName, int defaultValue) {
        String value = getString(parameterName);
        try {
            if (Integer.parseInt(value) < 1) {
                LOGGER.warn("The parameter " + parameterName + " is invalid, it must be greater than or equal to 1,"
                        + " will adopt it's default value: " + defaultValue);
                return false;
            }
        } catch (NumberFormatException e) {
            LOGGER.warn("The parameter " + parameterName + " is invalid, it must be integer,"
                    + " will adopt it's default value: " + defaultValue);
            return false;
        }
        return true;
    }

    private Boolean isBooleanValid(String parameterName) {
        String value = getString(parameterName);
        if (!value.equals("false") && !value.equals("true")) {
            LOGGER.warn("The parameter " + parameterName + " is invalid, it must be true or false,"
                    + " will adopt it's default value: false.");
            return false;
        }
        return true;
    }

    private void rectifyParameter() {
        if (isBooleanValid(APPEND_WRITE)) {
            isAppendWrite = Boolean.parseBoolean(getString(APPEND_WRITE));
        }
        if (isFilePathValid(PROCESS_FILE_PATH, sinkProcessFilePath)) {
            sinkProcessFilePath = getString(PROCESS_FILE_PATH);
        }
        if (isFilePathValid(CREATE_COUNT_INFO_PATH, createCountInfoPath)) {
            createCountInfoPath = getString(CREATE_COUNT_INFO_PATH);
        }
        if (isNumberValid(COMMIT_TIME_INTERVAL, commitTimeInterval)) {
            commitTimeInterval = Integer.parseInt(getString(COMMIT_TIME_INTERVAL));
        }
        if (isNumberValid(PROCESS_FILE_COUNT_LIMIT, processFileCountLimit)) {
            processFileCountLimit = Integer.parseInt(getString(PROCESS_FILE_COUNT_LIMIT));
        }
        if (isNumberValid(PROCESS_FILE_TIME_LIMIT, processFileTimeLimit)) {
            processFileTimeLimit = Integer.parseInt(getString(PROCESS_FILE_TIME_LIMIT));
        }
    }

    private void rectifyFailSqlPara() {
        if (isFilePathValid(FAIL_SQL_PATH, failSqlPath)) {
            failSqlPath = getString(FAIL_SQL_PATH);
        }
        if (isNumberValid(FILE_SIZE_LIMIT, fileSizeLimit)) {
            fileSizeLimit = Integer.parseInt(getString(FILE_SIZE_LIMIT));
        }
    }
}
