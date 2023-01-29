/**
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.opengauss.sink;

import io.debezium.connector.opengauss.sink.task.OpengaussSinkConnectorConfig;
import io.debezium.connector.opengauss.sink.task.OpengaussSinkConnectorTask;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class OpengaussSinkConnector extends SinkConnector {
    private static final Logger LOGGER = LoggerFactory.getLogger(OpengaussSinkConnector.class);

    private static final ConfigDef CONFIG_DEF = OpengaussSinkConnectorConfig.CONFIG_DEF;

    private Map<String, String> configProps;


    @Override
    public void start(Map<String, String> props) {
        configProps = props;

    }

    @Override
    public Class<? extends Task> taskClass() {
        return OpengaussSinkConnectorTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        LOGGER.info("Setting task configurations for {} workers.", maxTasks);
        final List<Map<String, String>> configs = new ArrayList<>(maxTasks);
        for (int i = 0; i < maxTasks; ++i) {
            configs.add(configProps);
        }
        return configs;
    }

    @Override
    public void stop() {

    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public String version() {
        return "1.0.0";
    }
}
