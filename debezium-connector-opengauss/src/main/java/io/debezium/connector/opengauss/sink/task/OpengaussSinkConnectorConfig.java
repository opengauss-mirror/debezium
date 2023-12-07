/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.opengauss.sink.task;

import io.debezium.config.SinkConnectorConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Description: OpengaussSinkConnectorConfig class
 *
 * @author wangzhengyuan
 * @date 2022/11/04
 */
public class OpengaussSinkConnectorConfig extends SinkConnectorConfig {
    /**
     * Max thread count
     */
    public static final String MAX_THREAD_COUNT = "max.thread.count";

    /**
     * Database type
     */
    public static final String DATABASE_TYPE = "database.type";

    /**
     * Database username
     */
    public static final String DATABASE_USERNAME = "database.username";

    /**
     * Database password
     */
    public static final String DATABASE_PASSWORD = "database.password";

    /**
     * Database ip
     */
    public static final String DATABASE_IP = "database.ip";

    /**
     * Database port
     */
    public static final String DATABASE_PORT = "database.port";

    /**
     * Database name
     */
    public static final String DATABASE_NAME = "database.name";

    /**
     * Whether to delete the csv file
     */
    public static final String DELETE_FULL_CSV_FILE = "delete.full.csv.file";

    /**
     * ConfigDef
     */
    public static final ConfigDef CONFIG_DEF = getConfigDef()
            .define(DELETE_FULL_CSV_FILE, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.HIGH,
            "whether to delete the csv file")
            .define(MAX_THREAD_COUNT, ConfigDef.Type.INT, ConfigDef.Importance.HIGH, "max thread count")
            .define(DATABASE_TYPE, ConfigDef.Type.STRING, "mysql", ConfigDef.Importance.HIGH,
                    "database type")
            .define(DATABASE_USERNAME, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
                    "database username")
            .define(DATABASE_PASSWORD, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
                    "database password")
            .define(DATABASE_IP, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "database ip")
            .define(DATABASE_PORT, ConfigDef.Type.INT, ConfigDef.Importance.HIGH, "database port")
            .define(DATABASE_NAME, ConfigDef.Type.STRING, "mysql", ConfigDef.Importance.HIGH,
                    "database name");

    private static final Logger LOGGER = LoggerFactory.getLogger(OpengaussSinkConnectorConfig.class);

    /**
     * maxThreadCount
     */
    public final Integer maxThreadCount;

    /**
     * databaseUsername
     */
    public final String databaseUsername;

    /**
     * databasePassword
     */
    public final String databasePassword;

    /**
     * databaseIp
     */
    public final String databaseIp;

    /**
     * databasePort
     */
    public final Integer databasePort;

    /**
     * Oracle database
     */
    public final String databaseName;

    /**
     * isDelCsv
     */
    public final boolean isDelCsv;

    /**
     * Database type
     */
    public final String databaseType;

    public OpengaussSinkConnectorConfig(Map<?, ?> props){
        super(CONFIG_DEF, props);
        this.maxThreadCount = getInt(MAX_THREAD_COUNT);

        this.databaseType = getString(DATABASE_TYPE);
        this.databaseUsername = getString(DATABASE_USERNAME);
        this.databasePassword = getString(DATABASE_PASSWORD);
        this.databaseIp = getString(DATABASE_IP);
        this.databasePort = getInt(DATABASE_PORT);
        this.databaseName = getString(DATABASE_NAME);

        this.isDelCsv = getBoolean(DELETE_FULL_CSV_FILE);

        Map<String, Object> allConfig = CONFIG_DEF.defaultValues();
        allConfig.forEach((k, v) -> {
            if (!props.containsKey(k)) {
                LOGGER.warn("The configuration {} item is not configured and uses the default value {}", k, v);
            }
        });
        props.forEach((k, v) -> {
            allConfig.put(String.valueOf(k), v);
        });
        logAll(allConfig, DATABASE_PASSWORD);
    }
}
