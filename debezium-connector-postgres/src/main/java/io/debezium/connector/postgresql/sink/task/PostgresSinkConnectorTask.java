/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package io.debezium.connector.postgresql.sink.task;

import java.util.Collection;
import java.util.Map;

import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.postgresql.Module;
import io.debezium.connector.postgresql.sink.PostgresReplayController;
import io.debezium.util.MigrationProcessController;

/**
 * Description: PostgresSinkConnectorTask class
 *
 * @author tianbin
 * @since 2024-11-25
 */
public class PostgresSinkConnectorTask extends SinkTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresSinkConnectorTask.class);
    private static final int COMMIT_RETRY_TIMES = 5;

    private final MigrationProcessController controller = new MigrationProcessController();
    private PostgresSinkConnectorConfig config;
    private PostgresReplayController postgresReplayController;
    private int count = 0;
    private int commitRetryTimes = 0;
    private long preOffset;
    private int pollIntervalSeconds;

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public void start(Map<String, String> props) {
        config = new PostgresSinkConnectorConfig(props);
        if ("******".equals(config.databasePassword)) {
            config.databasePassword = config.getPasswordByEnv();
        }
        controller.initParameter(config);
        postgresReplayController = new PostgresReplayController(config);
        postgresReplayController.createWorkThread();
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        controller.waitConnectionAlive(postgresReplayController.getConnectionStatus());
        while (postgresReplayController.isSinkQueueBlock() || postgresReplayController.isWorkQueueBlock()) {
            count++;
            if (count >= 300) {
                count = 0;
                LOGGER.warn("have wait 15s, so skip the loop");
                break;
            }
            MigrationProcessController.sleep(50);
        }
        count = 0;
        if (records == null || records.isEmpty()) {
            return;
        }
        postgresReplayController.batchWrite(records);
    }

    @Override
    public void stop() {
        postgresReplayController.doStop();
    }
}
