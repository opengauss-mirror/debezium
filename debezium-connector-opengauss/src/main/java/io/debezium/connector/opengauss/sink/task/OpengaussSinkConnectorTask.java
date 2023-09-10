package io.debezium.connector.opengauss.sink.task;

import io.debezium.connector.opengauss.sink.replay.JdbcDbWriter;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Description: OpengaussSinkConnectorTask class
 *
 * @author wangzhengyuan
 * @date 2022/11/05
 */
public class OpengaussSinkConnectorTask extends SinkTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(OpengaussSinkConnectorTask.class);
    private OpengaussSinkConnectorConfig config;
    private JdbcDbWriter jdbcDbWriter;
    private int count = 0;

    @Override
    public String version() {
        return "1.0.0";
    }

    @Override
    public void start(Map<String, String> props) {
        config = new OpengaussSinkConnectorConfig(props);
        jdbcDbWriter = new JdbcDbWriter(config);
        jdbcDbWriter.createWorkThread();
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        while (jdbcDbWriter.isSinkQueueBlock() || jdbcDbWriter.isWorkQueueBlock()) {
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
        if (records == null || records.isEmpty()){
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
            OffsetAndMetadata replayedOffset = new OffsetAndMetadata(minReplayedOffset, "");
            Set<TopicPartition> topicPartitions = currentOffsets.keySet();
            TopicPartition topicPartition = topicPartitions.iterator().next();
            preCommitOffsets.put(topicPartition, replayedOffset);
        } else {
            preCommitOffsets = currentOffsets;
        }
        LOGGER.warn("currentOffsets is {},preCommitOffsets is {}", currentOffsets, preCommitOffsets);
        this.flush(preCommitOffsets);
        return preCommitOffsets;
    }
}
