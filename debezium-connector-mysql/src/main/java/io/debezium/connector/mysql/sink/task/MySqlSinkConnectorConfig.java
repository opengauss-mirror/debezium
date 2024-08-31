/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.sink.task;

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.SinkConnectorConfig;

/**
 * Description: MySqlSinkConnectorConfig class
 * @author douxin
 * @date 2022/10/17
 **/
public class MySqlSinkConnectorConfig extends SinkConnectorConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(MySqlSinkConnectorConfig.class);

    /**
     * openGauss driver
     */
    public static final String OPENGAUSS_DRIVER = "opengauss.driver";

    /**
     * openGauss username
     */
    public static final String OPENGAUSS_USERNAME = "opengauss.username";

    /**
     * openGauss password
     */
    public static final String OPENGAUSS_PASSWORD = "opengauss.password";

    /**
     * openGauss url
     */
    public static final String OPENGAUSS_URL = "opengauss.url";

    /**
     * Parallel replay thread num
     */
    public static final String PARALLEL_REPLAY_THREAD_NUM = "parallel.replay.thread.num";

    /**
     * Parallel replay mode
     */
    public static final String PARALLEL_BASED_TRANSACTION = "provide.transaction.metadata";

    /**
     * Xlog location
     */
    public static final String XLOG_LOCATION = "xlog.location";

    /**
     * database standby hostnames
     */
    public static final String DB_STANDBY_HOSTNAMES = "database.standby.hostnames";

    /**
     * database standby ports
     */
    public static final String DB_STANDBY_PORTS = "database.standby.ports";

    /**
     * CONFIG_DEF
     */
    public static final ConfigDef CONFIG_DEF = getConfigDef()
            .define(OPENGAUSS_DRIVER, ConfigDef.Type.STRING, "org.opengauss.Driver",
                    ConfigDef.Importance.HIGH, "openGauss driver class name")
            .define(OPENGAUSS_USERNAME, ConfigDef.Type.STRING, "opengauss_user",
                    ConfigDef.Importance.HIGH, "openGauss username")
            .define(OPENGAUSS_PASSWORD, ConfigDef.Type.STRING, "******",
                    ConfigDef.Importance.HIGH, "openGauss password")
            .define(OPENGAUSS_URL, ConfigDef.Type.STRING,
                    "jdbc:opengauss://127.0.0.1:5432/migration?loggerLevel=OFF",
                    ConfigDef.Importance.HIGH, "openGauss url")
            .define(PARALLEL_REPLAY_THREAD_NUM, ConfigDef.Type.INT, 30,
                    ConfigDef.Importance.HIGH, "parallel replay thread num")
            .define(XLOG_LOCATION, ConfigDef.Type.STRING, getCurrentPluginPath(),
                    ConfigDef.Importance.HIGH, "xlog location")
            .define(DB_STANDBY_HOSTNAMES, ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM,
                    "database standby hostnames")
            .define(DB_STANDBY_PORTS, ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM,
                    "database standby ports");

    /**
     * openGauss driver
     */
    public final String openGaussDriver;

    /**
     * Username
     */
    public final String openGaussUsername;

    /**
     * Password
     */
    public final String openGaussPassword;

    /**
     * Connection url
     */
    public final String openGaussUrl;

    /**
     * Parallel replay thread num
     */
    public final int parallelReplayThreadNum;

    /**
     * Xlog location
     */
    public final String xlogLocation;

    /**
     * Is parallel based transaction
     */
    public final boolean isParallelBasedTransaction;

    private String dbStandbyHostnames;
    private String dbStandbyPorts;

    /**
     * Constructor
     *
     * @param props Map<?, ?> the props
     */
    public MySqlSinkConnectorConfig(Map<?, ?> props) {
        super(CONFIG_DEF, props);
        this.openGaussDriver = getString(OPENGAUSS_DRIVER);
        this.openGaussUsername = getString(OPENGAUSS_USERNAME);
        this.openGaussPassword = getString(OPENGAUSS_PASSWORD);
        this.openGaussUrl = getString(OPENGAUSS_URL);

        this.parallelReplayThreadNum = getInt(PARALLEL_REPLAY_THREAD_NUM);
        this.xlogLocation = getString(XLOG_LOCATION);
        this.isParallelBasedTransaction = Boolean.parseBoolean(configMap.get(PARALLEL_BASED_TRANSACTION));
        this.dbStandbyHostnames = getString(DB_STANDBY_HOSTNAMES);
        this.dbStandbyPorts = getString(DB_STANDBY_PORTS);

        Map<String, Object> allConfig = CONFIG_DEF.defaultValues();
        allConfig.forEach((k, v) -> {
            if (!props.containsKey(k)) {
                LOGGER.warn("The configuration {} item is not configured and uses the default value {}", k, v);
            }
        });
        props.forEach((k, v) -> {
            allConfig.put(String.valueOf(k), v);
        });
        logAll(allConfig, OPENGAUSS_PASSWORD);
    }

    /**
     * Get database standby hostname list
     *
     * @return String the standby cluster hostnames list
     */
    public String getDbStandbyHostnames() {
        return dbStandbyHostnames;
    }

    /**
     * Get database standby port list
     *
     * @return String the standby cluster port list
     */
    public String getDbStandbyPorts() {
        return dbStandbyPorts;
    }

    @Override
    protected void initDefaultConfigMap() {
        super.initDefaultConfigMap();
        configMap.put(PARALLEL_BASED_TRANSACTION, "false");
    }
}
