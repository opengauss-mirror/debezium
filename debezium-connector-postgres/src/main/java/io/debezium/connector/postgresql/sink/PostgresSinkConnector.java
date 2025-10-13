/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package io.debezium.connector.postgresql.sink;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.postgresql.sink.task.PostgresSinkConnectorConfig;
import io.debezium.connector.postgresql.sink.task.PostgresSinkConnectorTask;

/**
 * This class represents a PostgreSQL sink connector for Debezium.
 * It extends the SinkConnector class and provides functionality to connect to a PostgreSQL database.
 *
 * @author tianbin
 * @since 2024-11-25
 */
public class PostgresSinkConnector extends SinkConnector {
    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresSinkConnector.class);

    private static final ConfigDef CONFIG_DEF = PostgresSinkConnectorConfig.CONFIG_DEF;

    private Map<String, String> configProps;

    @Override
    public void start(Map<String, String> props) {
        configProps = props;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return PostgresSinkConnectorTask.class;
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
