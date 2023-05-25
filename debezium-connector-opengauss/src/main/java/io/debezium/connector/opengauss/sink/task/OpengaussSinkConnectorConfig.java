/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.opengauss.sink.task;

import io.debezium.config.SinkConnectorConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

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
     * Mysql port
     */
    public static final String PORT = "mysql.port";

    /**
     * ConfigDef
     */
    public static final ConfigDef CONFIG_DEF = getConfigDef()
            .define(MAX_THREAD_COUNT, ConfigDef.Type.INT, ConfigDef.Importance.HIGH, "max thread count")
            .define(MYSQL_USERNAME, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "mysql username")
            .define(MYSQL_PASSWORD, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "mysql password")
            .define(MYSQL_URL, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "mysql url")
            .define(PORT, ConfigDef.Type.INT, ConfigDef.Importance.HIGH, "mysql port");

    private static final Logger LOGGER = LoggerFactory.getLogger(OpengaussSinkConnectorConfig.class);

    /**
     * maxThreadCount
     */
    public final Integer maxThreadCount;

    /**
     * mysqlUsername
     */
    public final String mysqlUsername;

    /**
     * mysqlPassword
     */
    public final String mysqlPassword;

    /**
     * mysqlUrl
     */
    public final String mysqlUrl;

    /**
     * port
     */
    public final Integer port;

    private final Map<String, Object> values;

    public OpengaussSinkConnectorConfig(Map<?, ?> props){
        super(CONFIG_DEF, props);
        this.maxThreadCount = getInt(MAX_THREAD_COUNT);

        this.mysqlUsername = getString(MYSQL_USERNAME);
        this.mysqlPassword = getString(MYSQL_PASSWORD);
        this.mysqlUrl = getString(MYSQL_URL);
        this.port = getInt(PORT);

        this.values = (Map<String, Object>) props;
        logAll();
    }

    private void logAll() {
        StringBuilder b = new StringBuilder();
        b.append(this.getClass().getSimpleName());
        b.append(" values: ");
        b.append(Utils.NL);
        Iterator set = (new TreeMap(this.values)).entrySet().iterator();

        while(set.hasNext()) {
            Map.Entry<String, Object> entry = (Map.Entry)set.next();
            b.append('\t');
            b.append(entry.getKey());
            b.append(" = ");
            if (MYSQL_PASSWORD.equals(entry.getKey())){
                b.append("*******");
            } else {
                b.append(entry.getValue());
            }
            b.append(Utils.NL);
        }

        LOGGER.info(b.toString());
    }
}
