/*
 * Copyright 2026 Huawei Technologies Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.debezium.connector.postgresql.sink.task;

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.SinkConnectorConfig;

/**
 * Description: PostgresSinkConnectorConfig class
 *
 * @author tianbin
 * @since 2024-11-25
 */
public class PostgresSinkConnectorConfig extends SinkConnectorConfig {
    /**
     * Max thread count
     */
    public static final String MAX_THREAD_COUNT = "max.thread.count";

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
     * Xlog location
     */
    public static final String XLOG_LOCATION = "xlog.location";

    /**
     * Whether to delete the csv file
     */
    public static final String DELETE_FULL_CSV_FILE = "delete.full.csv.file";

    /**
     * The with options of create table
     */
    public static final String CREATE_TABLE_WITH_OPTIONS = "create.table.with.options";

    /**
     * ConfigDef
     */
    public static final ConfigDef CONFIG_DEF = getConfigDef()
            .define(DELETE_FULL_CSV_FILE, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.HIGH,
                    "whether to delete the csv file")
            .define(MAX_THREAD_COUNT, ConfigDef.Type.INT, ConfigDef.Importance.HIGH, "max thread count")
            .define(DATABASE_USERNAME, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
                    "database username")
            .define(DATABASE_PASSWORD, ConfigDef.Type.STRING, "******", ConfigDef.Importance.HIGH,
                    "database password")
            .define(DATABASE_IP, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "database ip")
            .define(DATABASE_PORT, ConfigDef.Type.INT, ConfigDef.Importance.HIGH, "database port")
            .define(DATABASE_NAME, ConfigDef.Type.STRING, "openGauss", ConfigDef.Importance.HIGH,
                    "database name")
            .define(XLOG_LOCATION, ConfigDef.Type.STRING, getCurrentPluginPath(),
                    ConfigDef.Importance.HIGH, "xlog location")
            .define(CREATE_COUNT_INFO_PATH, ConfigDef.Type.STRING, getCurrentPluginPath(), ConfigDef.Importance.MEDIUM,
                    "Create count info path")
            .define(CREATE_TABLE_WITH_OPTIONS, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM,
                    "The with options of create table");

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresSinkConnectorConfig.class);

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
    public String databasePassword;

    /**
     * databaseIp
     */
    public final String databaseIp;

    /**
     * databasePort
     */
    public final Integer databasePort;

    /**
     * database
     */
    public final String databaseName;

    /**
     * isDelCsv
     */
    public final boolean isDelCsv;

    /**
     * Xlog location
     */
    public final String xlogLocation;

    /**
     * The with options of create table
     */
    public final String createTableWithOptions;

    public PostgresSinkConnectorConfig(Map<?, ?> props) {
        super(CONFIG_DEF, props);
        this.maxThreadCount = getInt(MAX_THREAD_COUNT);
        this.databaseUsername = getString(DATABASE_USERNAME);
        this.databasePassword = getString(DATABASE_PASSWORD);
        this.databaseIp = getString(DATABASE_IP);
        this.databasePort = getInt(DATABASE_PORT);
        this.databaseName = getString(DATABASE_NAME);
        this.xlogLocation = getString(XLOG_LOCATION);
        this.isDelCsv = getBoolean(DELETE_FULL_CSV_FILE);
        this.createTableWithOptions = getString(CREATE_TABLE_WITH_OPTIONS);

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

    @Override
    protected void initCouplingConfig() {
        configMap.put(CREATE_COUNT_INFO_PATH, getString(CREATE_COUNT_INFO_PATH));
    }
}
