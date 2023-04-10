/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.sink.task;

import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Description: MySqlSinkConnectorConfig class
 * @author douxin
 * @date 2022/10/17
 **/
public class MySqlSinkConnectorConfig extends AbstractConfig {
    /**
     * Topics
     */
    public static final String TOPICS = "topics";

    /**
     * Max retries
     */
    public static final String MAX_RETRIES = "max.retries";

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
     * Xlog location
     */
    public static final String XLOG_LOCATION = "xlog.location";

    /**
     * Schema mappings
     */
    public static final String SCHEMA_MAPPINGS = "schema.mappings";

    public static ConfigDef CONFIG_DEF = new ConfigDef()
            .define(TOPICS, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "topics")
            .define(MAX_RETRIES, ConfigDef.Type.INT, ConfigDef.Importance.HIGH, "max retries")
            .define(OPENGAUSS_DRIVER, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "openGauss driver class name")
            .define(OPENGAUSS_USERNAME, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "openGauss username")
            .define(OPENGAUSS_PASSWORD, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "openGauss password")
            .define(OPENGAUSS_URL, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "openGauss url")
            .define(PARALLEL_REPLAY_THREAD_NUM, ConfigDef.Type.INT, 30, ConfigDef.Importance.HIGH, "parallel replay thread num")
            .define(XLOG_LOCATION, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "xlog location")
            .define(SCHEMA_MAPPINGS, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "schema mappings");

    private static final Logger LOGGER = LoggerFactory.getLogger(MySqlSinkConnectorConfig.class);

    /**
     * Topics
     */
    public final String topics;

    /**
     * Max retries
     */
    public final Integer maxRetries;

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
     * Schema mapping
     */
    public final String schemaMappings;

    /**
     * Constructor
     *
     * @param Map<?, ?> the props
     */
    public MySqlSinkConnectorConfig(Map<?, ?> props) {
        super(CONFIG_DEF, props, false);
        this.topics = getString(TOPICS);
        this.maxRetries = getInt(MAX_RETRIES);

        this.openGaussDriver = getString(OPENGAUSS_DRIVER);
        this.openGaussUsername = getString(OPENGAUSS_USERNAME);
        this.openGaussPassword = getString(OPENGAUSS_PASSWORD);
        this.openGaussUrl = getString(OPENGAUSS_URL);

        this.parallelReplayThreadNum = getInt(PARALLEL_REPLAY_THREAD_NUM);
        this.xlogLocation = getString(XLOG_LOCATION);
        this.schemaMappings = getString(SCHEMA_MAPPINGS);

        logAll(props);
    }

    private void logAll(Map<?, ?> props) {
        StringBuilder sb = new StringBuilder();
        sb.append(getClass().getSimpleName());
        sb.append(" values: ");
        sb.append(Utils.NL);

        for (Map.Entry entry : props.entrySet()) {
            sb.append('\t');
            sb.append(entry.getKey());
            sb.append(" = ");
            if (OPENGAUSS_PASSWORD.equals(entry.getKey())) {
                sb.append("********");
            }
            else {
                sb.append(entry.getValue());
            }
            sb.append(Utils.NL);
        }
        LOGGER.info(sb.toString());
    }
}
