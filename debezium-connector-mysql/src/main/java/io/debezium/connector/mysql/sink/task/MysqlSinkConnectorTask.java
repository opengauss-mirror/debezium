/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.sink.task;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.mysql.Module;
import io.debezium.connector.mysql.sink.replay.ReplayTask;
import io.debezium.connector.mysql.sink.replay.table.TableReplayTask;
import io.debezium.connector.mysql.sink.replay.transaction.TransactionReplayTask;

/**
 * Description: MysqlSinkConnectorTask class
 *
 * @author douxin
 * @since 2022/10/17
 **/
public class MysqlSinkConnectorTask extends SinkTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(MysqlSinkConnectorTask.class);
    private static final int COMMIT_RETRY_TIMES = 5;

    private MySqlSinkConnectorConfig config;
    private ReplayTask jdbcDbWriter;
    private int count = 0;
    private int commitRetryTimes = 0;
    private long preOffset;

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public void start(Map<String, String> props) {
        config = new MySqlSinkConnectorConfig(props);
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
                Thread.sleep(50);
            }
            catch (InterruptedException exp) {
                LOGGER.warn("Receive interrupted exception while put records from kafka.", exp.getMessage());
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

    @Override
    public Map<TopicPartition, OffsetAndMetadata> preCommit(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        Collection<OffsetAndMetadata> values = currentOffsets.values();
        List<OffsetAndMetadata> currentOffsetAndMetadataList = new ArrayList<>(values);
        OffsetAndMetadata offsetAndMetadata = currentOffsetAndMetadataList.get(0);
        long currentOffset = offsetAndMetadata.offset();
        Map<TopicPartition, OffsetAndMetadata> preCommitOffsets = new HashMap<>();
        Long minReplayedOffset = jdbcDbWriter.getReplayedOffset();
        if (minReplayedOffset < currentOffset && minReplayedOffset > 0L) {
            if (minReplayedOffset.equals(preOffset)) {
                commitRetryTimes++;
            } else {
                preOffset = minReplayedOffset;
                commitRetryTimes = 0;
            }
            if (commitRetryTimes == COMMIT_RETRY_TIMES) {
                LOGGER.warn("commit the same offset [{}] times, maybe occur kafka exception and clear buffer, "
                            + "currentOffsets is {}", COMMIT_RETRY_TIMES, currentOffsets);
                jdbcDbWriter.clearReplayedOffset(currentOffset);
                this.flush(preCommitOffsets);
                return currentOffsets;
            }
            OffsetAndMetadata replayedOffset = new OffsetAndMetadata(minReplayedOffset, "");
            Set<TopicPartition> topicPartitions = currentOffsets.keySet();
            TopicPartition topicPartition = topicPartitions.iterator().next();
            preCommitOffsets.put(topicPartition, replayedOffset);
        }
        else {
            preCommitOffsets = currentOffsets;
        }
        LOGGER.warn("currentOffsets is {},preCommitOffsets is {}", currentOffsets, preCommitOffsets);
        this.flush(preCommitOffsets);
        return preCommitOffsets;
    }
}
