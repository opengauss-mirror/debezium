/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.config;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

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
            .define(COMMIT_PROCESS_WHILE_RUNNING, ConfigDef.Type.BOOLEAN, false,
                    ConfigDef.Importance.HIGH, "commit process while running")
            .define(PROCESS_FILE_PATH, ConfigDef.Type.STRING, getCurrentPluginPath() + "sink" + File.separator,
                    ConfigDef.Importance.HIGH, "sink process file path")
            .define(FAIL_SQL_PATH, ConfigDef.Type.STRING, getCurrentPluginPath() + "fail-sqls" + File.separator,
                    ConfigDef.Importance.HIGH, "fail sql file path")
            .define(COMMIT_TIME_INTERVAL, ConfigDef.Type.INT, 1, ConfigDef.Importance.HIGH, "commit time interval")
            .define(CREATE_COUNT_INFO_PATH, ConfigDef.Type.STRING, getCurrentPluginPath(),
                    ConfigDef.Importance.HIGH, "create count information path")
            .define(PROCESS_FILE_COUNT_LIMIT, ConfigDef.Type.INT, 100,
                    ConfigDef.Importance.HIGH, "process file count limit")
            .define(PROCESS_FILE_TIME_LIMIT, ConfigDef.Type.INT, 168,
                    ConfigDef.Importance.HIGH, "process file time limit")
            .define(APPEND_WRITE, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.HIGH, "append write")
            .define(FILE_SIZE_LIMIT, ConfigDef.Type.INT, 10, ConfigDef.Importance.HIGH, "file size limit");

    /**
     * Topics
     */
    public final String topics;

    /**
     * Schema mapping
     */
    public final String schemaMappings;

    /**
     * commit process while running
     */

    public final boolean isCommitProcess;

    /**
     * sink process
     */
    public final String sinkProcessFilePath;

    /**
     * fail sql file path
     */
    public final String failSqlPath;

    /**
     * commit time interval
     */

    public final int commitTimeInterval;

    /**
     * create count information path
     */
    public final String createCountInfoPath;

    /**
     * process file count limit
     */
    public final int processFileCountLimit;

    /**
     * process file time limit
     */
    public final int processFileTimeLimit;

    /**
     * is append write
     */
    public final boolean isAppendWrite;

    /**
     * file size limit
     */
    public final int fileSizeLimit;

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
        this.isCommitProcess = getBoolean(COMMIT_PROCESS_WHILE_RUNNING);
        this.sinkProcessFilePath = getString(PROCESS_FILE_PATH);
        this.failSqlPath = getString(FAIL_SQL_PATH);
        this.commitTimeInterval = getInt(COMMIT_TIME_INTERVAL);
        this.createCountInfoPath = getString(CREATE_COUNT_INFO_PATH);
        this.processFileCountLimit = getInt(PROCESS_FILE_COUNT_LIMIT);
        this.processFileTimeLimit = getInt(PROCESS_FILE_TIME_LIMIT);
        this.isAppendWrite = getBoolean(APPEND_WRITE);
        this.fileSizeLimit = getInt(FILE_SIZE_LIMIT);
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
}
