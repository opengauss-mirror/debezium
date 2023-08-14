/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.config;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.kafka.KafkaClient;
import io.debezium.util.Strings;

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
     * breakpoint switch
     */
    public static final String BP_SWITCH = "record.breakpoint.kafka.switch";

    /**
     * breakpoint topic
     */
    public static final String BP_TOPIC = "record.breakpoint.kafka.topic";

    /**
     * breakpoint attempts
     */
    public static final String BP_ATTEMPTS = "record.breakpoint.kafka.recovery.attempts";

    /**
     * breakpoint kafka server
     */
    public static final String BP_BOOTSTRAP_SERVERS = "record.breakpoint.kafka.bootstrap.servers";

    /**
     * breakpoint kafka size limit
     */
    public static final String BP_QUEUE_MAX_SIZE = "record.breakpoint.kafka.size.limit";

    /**
     * breakpoint kafka size limit
     */
    public static final String BP_QUEUE_CLEAR_INTERVAL = "record.breakpoint.kafka.clear.interval";

    /**
     * Max Queue size
     */
    public static final String MAX_QUEUE_SIZE = "max.queue.size";

    /**
     * Open flow control threshold
     */
    public static final String OPEN_FLOW_CONTROL_THRESHOLD = "open.flow.control.threshold";

    /**
     * Close flow control threshold
     */
    public static final String CLOSE_FLOW_CONTROL_THRESHOLD = "close.flow.control.threshold";

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
            .define(PROCESS_FILE_COUNT_LIMIT, ConfigDef.Type.STRING, "10",
                    ConfigDef.Importance.HIGH, "process file count limit")
            .define(PROCESS_FILE_TIME_LIMIT, ConfigDef.Type.STRING, "168",
                    ConfigDef.Importance.HIGH, "process file time limit")
            .define(APPEND_WRITE, ConfigDef.Type.STRING, "false", ConfigDef.Importance.HIGH, "append write")
            .define(FILE_SIZE_LIMIT, ConfigDef.Type.STRING, "10", ConfigDef.Importance.HIGH, "file size limit")
            .define(BP_BOOTSTRAP_SERVERS, ConfigDef.Type.STRING, "localhost:9092",
                    ConfigDef.Importance.HIGH, "breakpoint kafka server")
            .define(BP_TOPIC, ConfigDef.Type.STRING, "bp_topic", ConfigDef.Importance.HIGH, "breakpoint topic")
            .define(BP_ATTEMPTS, ConfigDef.Type.STRING, "3", ConfigDef.Importance.HIGH, "breakpoint attempts")
            .define(BP_QUEUE_MAX_SIZE, ConfigDef.Type.STRING, "3000",
                    ConfigDef.Importance.HIGH, "Exceeding this limit deletes the breakpoint record")
            .define(BP_QUEUE_CLEAR_INTERVAL, ConfigDef.Type.STRING, "1",
                    ConfigDef.Importance.HIGH, "Exceeding this time limit deletes the breakpoint record")
            .define(MAX_QUEUE_SIZE, ConfigDef.Type.INT, 1000000, ConfigDef.Importance.HIGH, "max queue size")
            .define(OPEN_FLOW_CONTROL_THRESHOLD, ConfigDef.Type.DOUBLE, 0.8, ConfigDef.Importance.HIGH,
                    "open flow control threshold")
            .define(CLOSE_FLOW_CONTROL_THRESHOLD, ConfigDef.Type.DOUBLE, 0.7, ConfigDef.Importance.HIGH,
                    "close flow control threshold");
    private static final Logger LOGGER = LoggerFactory.getLogger(SinkConnectorConfig.class);

    /**
     * Topics
     */
    public final String topics;

    /**
     * Schema mapping
     */
    public final String schemaMappings;

    /**
     * Max queue size
     */
    public final int maxQueueSize;

    /**
     * Open flow control threshold
     */
    public final double openFlowControlThreshold;

    /**
     * Close flow control threshold
     */
    public final double closeFlowControlThreshold;

    /**
     * Config map
     */
    protected Map<String, String> configMap = new HashMap<>();
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
     * breakpoint config
     */
    private String bpTopic = "bp_topic";
    private String bootstrapServers = "localhost:9092";
    private int bpMaxRetries = 3;
    private int bpQueueSizeLimit = 3000;
    private int bpQueueTimeLimit = 1;

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
        this.bootstrapServers = getBootstrapServers();
        this.maxQueueSize = getInt(MAX_QUEUE_SIZE);
        this.openFlowControlThreshold = getDouble(OPEN_FLOW_CONTROL_THRESHOLD);
        this.closeFlowControlThreshold = getDouble(CLOSE_FLOW_CONTROL_THRESHOLD);
        initCouplingConfig();
        rectifyParameter();
    }

    private void initCouplingConfig() {
        KafkaClient client = new KafkaClient(bootstrapServers, topics);
        String sourceConfig = client.readSourceConfig();
        String[] configs = sourceConfig.split(System.lineSeparator());
        if (!"".equals(sourceConfig)) {
            for (String value : configs) {
                configMap.put(value.trim().split("=")[0], value.trim().split("=")[1]);
            }
        }
        else {
            initDefaultConfigMap();
        }
    }

    protected void logAll(Map<?, ?> props, String name) {
        StringBuilder sb = new StringBuilder();
        sb.append(getClass().getSimpleName());
        sb.append(" values: ");
        sb.append(Utils.NL);

        for (Map.Entry entry : props.entrySet()) {
            sb.append('\t');
            sb.append(entry.getKey());
            sb.append(" = ");
            if (name.equals(entry.getKey())) {
                sb.append("********");
            }
            else {
                sb.append(entry.getValue());
            }
            sb.append(Utils.NL);
        }
        LOGGER.info(sb.toString());
    }

    /**
     * Gets TOPICS.
     *
     * @return the value of TOPICS
     */
    public static String getTOPICS() {
        return TOPICS;
    }

    /**
     * Gets bpTopic.
     *
     * @return the value of bpTopic
     */
    public String getBpTopic() {
        return bpTopic;
    }

    /**
     * Gets bootstrapServers.
     *
     * @return the value of bootstrapServers
     */
    public String getBootstrapServers() {
        if (isSeverPathValid(BP_BOOTSTRAP_SERVERS, bootstrapServers)) {
            bootstrapServers = getString(BP_BOOTSTRAP_SERVERS);
        }
        return bootstrapServers;
    }

    /**
     * Gets bpMaxRetries.
     *
     * @return the value of bpMaxRetries
     */
    public int getBpMaxRetries() {
        return bpMaxRetries;
    }

    /**
     * Sets the bpMaxRetries.
     *
     * @param bpMaxRetries the value of bpMaxRetries
     */
    public void setBpMaxRetries(int bpMaxRetries) {
        this.bpMaxRetries = bpMaxRetries;
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

    /**
     * Gets bpQueueSizeLimit.
     *
     * @return the value of bpQueueSizeLimit
     */
    public int getBpQueueSizeLimit() {
        return bpQueueSizeLimit;
    }

    /**
     * Gets bpQueueTimeLimit.
     *
     * @return the value of bpQueueTimeLimit
     */
    public int getBpQueueTimeLimit() {
        return bpQueueTimeLimit;
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
        }
        catch (NumberFormatException e) {
            LOGGER.warn("The parameter " + parameterName + " is invalid, it must be integer,"
                    + " will adopt it's default value: " + defaultValue);
            return false;
        }
        return true;
    }

    private Boolean isSeverPathValid(String parameterName, String defaultValue) {
        String value = getString(parameterName);
        if (value.contains("localhost")) {
            value = value.replace("localhost", "127.0.0.1");
        }
        String regex = "((25[0-5]|2[0-4]\\d|1\\d{2}|[1-9]?\\d)\\.)"
                + "{3}(25[0-5]|2[0-4]\\d|1\\d{2}|[1-9]?\\d)"
                + "(:([0-9]|[1-9]\\d|[1-9]\\d{2}|[1-9]\\d{3}|[1-5]\\d{4}|6[0-4]\\d{2}|655[0-2]\\d|6553[0-5])$)";
        if ("".equals(value) || !value.matches(regex)) {
            LOGGER.warn("The parameter " + parameterName + " is invalid, it must be server path,"
                    + " will adopt it's default value: " + defaultValue);
            return false;
        }
        return true;
    }

    private Boolean isStringValid(String parameterName, String defaultValue) {
        String value = getString(parameterName);
        if (Strings.isNullOrEmpty(value)) {
            LOGGER.warn("The parameter " + parameterName + " is invalid, it must be string,"
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
        if (isCommitProcess()) {
            if (isBooleanValid(APPEND_WRITE)) {
                isAppendWrite = Boolean.parseBoolean(getString(APPEND_WRITE));
            }
            if (isFilePathValid(PROCESS_FILE_PATH, sinkProcessFilePath)) {
                sinkProcessFilePath = getString(PROCESS_FILE_PATH);
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
            this.createCountInfoPath = configMap.get(CREATE_COUNT_INFO_PATH);
        }
        if (isStringValid(BP_TOPIC, bpTopic)) {
            bpTopic = getString(BP_TOPIC);
        }
        if (isNumberValid(BP_ATTEMPTS, bpMaxRetries)) {
            bpMaxRetries = Integer.parseInt(getString(BP_ATTEMPTS));
        }
        if (isNumberValid(BP_QUEUE_MAX_SIZE, bpQueueSizeLimit)) {
            bpQueueSizeLimit = Integer.parseInt(getString(BP_QUEUE_MAX_SIZE)) * 10000;
        }
        if (isNumberValid(BP_QUEUE_CLEAR_INTERVAL, bpQueueTimeLimit)) {
            bpQueueTimeLimit = Integer.parseInt(getString(BP_QUEUE_CLEAR_INTERVAL));
        }
        if (isFilePathValid(FAIL_SQL_PATH, failSqlPath)) {
            failSqlPath = getString(FAIL_SQL_PATH);
        }
        if (isNumberValid(FILE_SIZE_LIMIT, fileSizeLimit)) {
            fileSizeLimit = Integer.parseInt(getString(FILE_SIZE_LIMIT));
        }
    }

    /**
     * Ininialize config map
     */
    protected void initDefaultConfigMap() {
        configMap.put(CREATE_COUNT_INFO_PATH, getCurrentPluginPath());
    }
}
