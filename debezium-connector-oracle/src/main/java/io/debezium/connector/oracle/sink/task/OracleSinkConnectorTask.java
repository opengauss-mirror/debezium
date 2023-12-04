/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.sink.task;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.oracle.Module;
import io.debezium.connector.oracle.sink.replay.ReplayTask;
import io.debezium.connector.oracle.sink.replay.table.TableReplayTask;
import io.debezium.connector.oracle.sink.replay.transaction.TransactionReplayTask;

/**
 * Description: OracleSinkConnectorTask
 *
 * @author gbase
 * @date 2023/07/28
 **/
public class OracleSinkConnectorTask extends SinkTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(OracleSinkConnectorTask.class);

    private ReplayTask jdbcDbWriter;
    private int count = 0;

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public void start(Map<String, String> props) {
        OracleSinkConnectorConfig config = new OracleSinkConnectorConfig(props);
        if (config.isParallelBasedTransaction) {
            jdbcDbWriter = new TransactionReplayTask(config);
        }
        else {
            jdbcDbWriter = new TableReplayTask(config);
        }
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
                TimeUnit.MILLISECONDS.sleep(50);
            }
            catch (InterruptedException exp) {
                LOGGER.warn("Receive interrupted exception while put records from kafka:{}", exp.getMessage());
            }
        }
        count = 0;
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
