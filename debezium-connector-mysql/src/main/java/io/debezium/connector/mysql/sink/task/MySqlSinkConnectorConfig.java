/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.sink.task;

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.Utils;
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
    public static final String PARALLEL_BASED_TRANSACTION = "parallel.based.transaction";

    /**
     * Xlog location
     */
    public static final String XLOG_LOCATION = "xlog.location";

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
            .define(PARALLEL_BASED_TRANSACTION, ConfigDef.Type.BOOLEAN, true,
                    ConfigDef.Importance.HIGH, "parallel based transaction")
            .define(XLOG_LOCATION, ConfigDef.Type.STRING, getCurrentPluginPath(),
                    ConfigDef.Importance.HIGH, "xlog location")
            .define(MAX_QUEUE_SIZE, ConfigDef.Type.INT, 1000000,
                    ConfigDef.Importance.HIGH, "max queue size")
            .define(OPEN_FLOW_CONTROL_THRESHOLD, ConfigDef.Type.DOUBLE, 0.8,
                    ConfigDef.Importance.HIGH, "open flow control threshold")
            .define(CLOSE_FLOW_CONTROL_THRESHOLD, ConfigDef.Type.DOUBLE, 0.7,
                    ConfigDef.Importance.HIGH, "close flow control threshold");

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
     * Is parallel based transaction
     */
    public final boolean isParallelBasedTransaction;

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
        this.isParallelBasedTransaction = getBoolean(PARALLEL_BASED_TRANSACTION);
        this.xlogLocation = getString(XLOG_LOCATION);

        this.maxQueueSize = getInt(MAX_QUEUE_SIZE);
        this.openFlowControlThreshold = getDouble(OPEN_FLOW_CONTROL_THRESHOLD);
        this.closeFlowControlThreshold = getDouble(CLOSE_FLOW_CONTROL_THRESHOLD);

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
