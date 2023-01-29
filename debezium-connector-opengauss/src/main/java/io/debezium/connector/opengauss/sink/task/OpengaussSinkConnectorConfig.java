/**
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.opengauss.sink.task;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

/**
 * Description: OpengaussSinkConnectorConfig class
 * @author wangzhengyuan
 * @date 2022/11/04
 */
public class OpengaussSinkConnectorConfig extends AbstractConfig {
    public final String topics;
    public final Integer maxRetries;
    public final Integer maxThreadCount;

    public final String mysqlUsername;
    public final String mysqlPassword;
    public final String mysqlUrl;
    public final String mysqlDatabase;
    public final Integer port;

    public OpengaussSinkConnectorConfig(Map<?, ?> props){
        super(CONFIG_DEF, props);
        this.topics = getString(TOPICS);
        this.maxRetries = getInt(MAX_RETRIES);
        this.maxThreadCount = getInt(MAX_THREAD_COUNT);

        this.mysqlUsername = getString(MYSQL_USERNAME);
        this.mysqlPassword = getString(MYSQL_PASSWORD);
        this.mysqlUrl = getString(MYSQL_URL);
        this.mysqlDatabase = getString(MYSQL_DATABASE);
        this.port = getInt(PORT);

    }

    /**
     * Topics
     */
    public static final String TOPICS = "topics";

    /**
     * Max retries
     */
    public static final String MAX_RETRIES = "max_retries";

    /**
     * Max thread count
     */
    public static final String MAX_THREAD_COUNT = "max_thread_count";

    /**
     * Mysql username
     */
    public static final String MYSQL_USERNAME = "mysql.username";

    /**
     * Mysql password
     */
    public static final String MYSQL_PASSWORD = "mysql.password";

    /**
     * Mysql url
     */
    public static final String MYSQL_URL = "mysql.url";

    /**
     * Mysql database
     */
    public static final String MYSQL_DATABASE = "mysql.database";

    /**
     * Mysql port
     */
    public static final String PORT = "mysql.port";

    public static ConfigDef CONFIG_DEF = new ConfigDef()
            .define(TOPICS, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "topics")
            .define(MAX_RETRIES, ConfigDef.Type.INT, ConfigDef.Importance.HIGH, "max retries")
            .define(MAX_THREAD_COUNT, ConfigDef.Type.INT, ConfigDef.Importance.HIGH, "max thread count")
            .define(MYSQL_USERNAME, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "mysql username")
            .define(MYSQL_PASSWORD, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "mysql password")
            .define(MYSQL_URL, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "mysql url")
            .define(MYSQL_DATABASE, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "mysql database")
            .define(PORT, ConfigDef.Type.INT, ConfigDef.Importance.HIGH, "mysql port");
}
