/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.sink.task;

import java.util.Collection;
import java.util.Map;

import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.mysql.Module;
import io.debezium.connector.mysql.sink.replay.JdbcDbWriter;

/**
 * Description: MysqlSinkConnectorTask class
 *
 * @author douxin
 * @since 2022/10/17
 **/
public class MysqlSinkConnectorTask extends SinkTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(MysqlSinkConnectorTask.class);

    private MySqlSinkConnectorConfig config;
    private JdbcDbWriter jdbcDbWriter;
    private int count = 0;

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public void start(Map<String, String> props) {
        config = new MySqlSinkConnectorConfig(props);
        jdbcDbWriter = new JdbcDbWriter(config);
        jdbcDbWriter.createWorkThreads();
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        while (jdbcDbWriter.isBlock()) {
            count++;
            if (count >= 300) {
                count = 0;
                LOGGER.warn("have wait 15s, so skip the loop");
                break;
            }
            try {
                Thread.sleep(50);
            }
            catch (InterruptedException exp) {
                LOGGER.warn("Receive interrupted exception while put records from kafka.", exp.getMessage());
            }
        }
        if (records == null || records.isEmpty()) {
            return;
        }
        jdbcDbWriter.batchWrite(records);
    }

    @Override
    public void stop() {
        jdbcDbWriter.doStop();
    }
}
