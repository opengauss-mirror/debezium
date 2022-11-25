/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.sink.task;

import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

/**
 * Description: MySqlSinkConnectorConfig class
 * @author douxin
 * @date 2022/10/17
 **/
public class MySqlSinkConnectorConfig extends AbstractConfig {
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
     * Constructor
     *
     * @param Map<?, ?> the props
     */
    public MySqlSinkConnectorConfig(Map<?, ?> props) {
        super(CONFIG_DEF, props);
        this.topics = getString(TOPICS);
        this.maxRetries = getInt(MAX_RETRIES);

        this.openGaussDriver = getString(OPENGAUSS_DRIVER);
        this.openGaussUsername = getString(OPENGAUSS_USERNAME);
        this.openGaussPassword = getString(OPENGAUSS_PASSWORD);
        this.openGaussUrl = getString(OPENGAUSS_URL);
    }

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

    public static ConfigDef CONFIG_DEF = new ConfigDef()
            .define(TOPICS, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "topics")
            .define(MAX_RETRIES, ConfigDef.Type.INT, ConfigDef.Importance.HIGH, "max retries")
            .define(OPENGAUSS_DRIVER, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "openGauss driver class name")
            .define(OPENGAUSS_USERNAME, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "openGauss username")
            .define(OPENGAUSS_PASSWORD, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "openGauss password")
            .define(OPENGAUSS_URL, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "openGauss url");
}
