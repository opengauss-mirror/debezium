/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.breakpoint;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import io.debezium.config.SinkConnectorConfig;
import io.debezium.connector.kafka.KafkaClient;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DeleteRecordsResult;
import org.apache.kafka.clients.admin.DeletedRecords;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Queues;

import io.debezium.config.Field;

/**
 * Description: BreakPointRecord
 *
 * @author Lvlintao
 * @since 2023/05/10
 **/

public class BreakPointRecord {
    private static final Logger LOGGER = LoggerFactory.getLogger(BreakPointRecord.class);

    /**
     * The one and only partition of the breakpoint record topic.
     */
    public static final String CONFIGURATION_FIELD_PREFIX_STRING = "record.breakpoint.";

    /**
     * The breakpoint name
     */
    public static final Field NAME = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "name")
            .withDisplayName("Logical name for the breakpoint record")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("The name used for the breakpoint record, perhaps differently by each implementation.")
            .withValidation(Field::isOptional);

    /**
     * The breakpoint kafka recovery poll interval
     */
    public static final Field RECOVERY_POLL_INTERVAL_MS = Field.create(CONFIGURATION_FIELD_PREFIX_STRING
            + "kafka.recovery.poll.interval.ms")
            .withDisplayName("Poll interval during breakpoint recovery (ms)")
            .withType(ConfigDef.Type.INT)
            .withGroup(Field.createGroupEntry(Field.Group.ADVANCED, 1))
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("The number of milliseconds to wait while polling for persisted data during recovery.")
            .withDefault(100)
            .withValidation(Field::isNonNegativeInteger);

    /**
     * The breakpoint kafka recovery poll attempts
     */
    public static final Field RECOVERY_POLL_ATTEMPTS = Field.create(CONFIGURATION_FIELD_PREFIX_STRING
            + "kafka.recovery.attempts")
            .withDisplayName("Max attempts to recovery breakpoint record")
            .withType(ConfigDef.Type.INT)
            .withGroup(Field.createGroupEntry(Field.Group.ADVANCED, 0))
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("The number of attempts in a row that no data are returned from "
                    + "Kafka before recover completes. "
                    + "The maximum amount of time to wait after receiving no data is (recovery.attempts) "
                    + "x (recovery.poll.interval.ms).")
            .withDefault(100)
            .withValidation(Field::isInteger);
    private static final Long UNLIMITED_VALUE = -1L;

    private SinkConnectorConfig config;
    private KafkaClient client;
    private LinkedList<String> breakpointFilterList = new LinkedList<>();
    private Map<Long, Boolean> breakpointFilterMap = new HashMap<>();
    private BlockingQueue<BreakPointInfo> storeToKafkaQueue = new LinkedBlockingQueue<>();
    private final ThreadPoolExecutor threadPool = new ThreadPoolExecutor(2, 2, 100,
            TimeUnit.SECONDS, new LinkedBlockingQueue<>(2));
    private List<Long> toDeleteOffsets = new ArrayList<>();
    private Duration pollInterval;
    private int maxRecoveryAttempts;
    private int bpQueueTimeLimit;
    private int bpQueueSizeLimit;
    private Long totalMessageCount = 0L;
    private PriorityBlockingQueue<Long> replayedOffsets;
    private boolean isGetBp;
    private Long breakpointEndOffset = UNLIMITED_VALUE;
    private KafkaConsumer<String, String> bpRecordConsumer;
    private KafkaConsumer<String, String> breakpointConsumer;
    private KafkaConsumer<String, String> preDelConsumer;

    /**
     * configure the breakpoint properties
     *
     * @param connectorConfig  SinkConnectorConfig the connector config
     */
    public BreakPointRecord(SinkConnectorConfig connectorConfig) {
        this.config = connectorConfig;
        this.replayedOffsets = new PriorityBlockingQueue<>();
        this.pollInterval = Duration.ofMillis(500);
        this.maxRecoveryAttempts = connectorConfig.getBpMaxRetries();
        this.client = new KafkaClient(connectorConfig, CONFIGURATION_FIELD_PREFIX_STRING);
    }

    /**
     * get the breakpoint to delete last offset
     *
     * @return PriorityBlockingQueue bpDeleteOffsets
     */
    public PriorityBlockingQueue<Long> getReplayedOffset() {
        return this.replayedOffsets;
    }

    /**
     * Gets toDeleteOffsets.
     *
     * @return the value of toDeleteOffsets
     */
    public List<Long> getToDeleteOffsets() {
        return toDeleteOffsets;
    }

    /**
     * Start the record breakpoint info.
     */
    public synchronized void start() {
        this.bpQueueTimeLimit = config.getBpQueueTimeLimit();
        this.bpQueueSizeLimit = config.getBpQueueSizeLimit();
        storeToKafkaThread();
        deleteBpByTimeTask();
        deleteBpBySizeTask();
        client.initializeStorage(UNLIMITED_VALUE);
    }

    /**
     * Determine if there is a breakpoint condition
     *
     * @param records source connector collected data
     * @return boolean breakpoint situation
     */
    public boolean isExists(Collection<SinkRecord> records) {
        if (client.isTopicExist() && breakpointEndOffset.equals(UNLIMITED_VALUE)) {
            if (Objects.isNull(breakpointConsumer)) {
                breakpointConsumer = client.getConsumer();
            }
            long lastProcessedOffset = UNLIMITED_VALUE;
            Long endOffset = null;
            int recoveryAttempts = 0;
            do {
                if (recoveryAttempts > maxRecoveryAttempts) {
                    LOGGER.warn("The breakpoint record couldn't be read. Consider to increase the value for "
                            + RECOVERY_POLL_INTERVAL_MS.name());
                    break;
                }
                int numRecordsProcessed = 0;
                endOffset = getEndOffsetOfDbBreakPointTopic(endOffset, breakpointConsumer);
                ConsumerRecords<String, String> breakpointRecords = breakpointConsumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> record : breakpointRecords) {
                    try {
                        if (lastProcessedOffset < record.offset()) {
                            // the transactional record key is "beginOffset-endOffset",
                            // Non-transactional record key is "kafkaOffset"
                            if (record.key().contains("-")) {
                                int index = record.key().lastIndexOf("-");
                                long recordEndOffset = Long.parseLong(record.key().substring(index + 1));
                                breakpointEndOffset = Math.max(breakpointEndOffset, recordEndOffset);
                            }
                            else {
                                long recordEndOffset = Long.parseLong(record.key());
                                breakpointEndOffset = Math.max(breakpointEndOffset, recordEndOffset);
                            }
                        }
                        lastProcessedOffset = record.offset();
                        ++numRecordsProcessed;
                    }
                    catch (NumberFormatException e) {
                        LOGGER.error("NumberFormat exception while processing record '{}'", record, e);
                    }
                }
                if (numRecordsProcessed == 0) {
                    LOGGER.debug("No new records found in the breakpoint record; will retry");
                    recoveryAttempts++;
                }
                else {
                    LOGGER.debug("Processed {} records from breakpoint record", numRecordsProcessed);
                }
            } while (lastProcessedOffset < endOffset - 1);
        }
        return isExistBp(records);
    }

    private boolean isExistBp(Collection<SinkRecord> records) {
        Iterator<SinkRecord> itr = records.iterator();
        SinkRecord firstSinkRecord = itr.hasNext() ? itr.next() : null;
        boolean isExists = false;
        if (firstSinkRecord != null) {
            LOGGER.warn("breakpoint endOffset is {},records first offset is {}",
                    breakpointEndOffset, firstSinkRecord.kafkaOffset());
            isExists = breakpointEndOffset >= firstSinkRecord.kafkaOffset();
        }
        return isExists;
    }

    /**
     * Store breakpoint object to kafka topic
     *
     * @param record the replayed record
     * @param isTransaction boolean whether it is transaction
     */
    public void storeRecord(BreakPointObject record, Boolean isTransaction) {
        storeRecord(record, isTransaction, false);
    }

    /**
     * Store breakpoint object to kafka topic
     *
     * @param record the replayed record
     * @param isTransaction boolean whether it is transaction
     * @param isFull boolean whether it is full
     */
    public void storeRecord(BreakPointObject record, Boolean isTransaction, boolean isFull) {
        LOGGER.debug("Storing Breakpoint record into breakpoint topic: {}", record.toString());
        String key = "";
        String value = "";
        if (isTransaction) {
            key = record.getBeginOffset() + "-" + record.getEndOffset();
            value = "beginOffset=" + record.getBeginOffset()
                    + ", endOffset=" + record.getEndOffset()
                    + ", gtid=" + record.getGtid()
                    + ", timestamp=" + record.getTimeStamp();
        }
        else {
            key = record.getBeginOffset().toString();
            if (record.getLsn() != null) {
                value = "kafkaOffset=" + record.getBeginOffset()
                        + ", lsn=" + record.getLsn()
                        + ", timestamp=" + record.getTimeStamp();
            }
            if (record.getGtid() != null) {
                value = "kafkaOffset=" + record.getBeginOffset()
                        + ", gtid=" + record.getGtid()
                        + ", timestamp=" + record.getTimeStamp();
            }
        }
        if (isFull) {
            storeRecordToKafka(key, value);
        }
        else {
            storeToKafkaQueue.add(
                    new BreakPointInfo()
                            .setKey(key)
                            .setValue(value));
        }
    }

    private void storeToKafkaThread() {
        threadPool.execute(() -> {
            Thread.currentThread().setName("send2kafka-thread");
            List<BreakPointInfo> toStoreList = new ArrayList<>();
            while (true) {
                try {
                    Queues.drain(storeToKafkaQueue, toStoreList, 3000, 100, TimeUnit.MILLISECONDS);
                    for (BreakPointInfo breakPointInfo : toStoreList) {
                        storeRecordToKafka(breakPointInfo.getKey(), breakPointInfo.getValue());
                    }
                    client.getProducer().flush();
                    toStoreList.clear();
                }
                catch (InterruptedException e) {
                    LOGGER.error("occurred exception is {}", e.getMessage());
                }
            }
        });
    }

    /**
     * send record to kafka breakpoint topic
     *
     * @param key the sink record kafka info
     * @param value the breakpoint info
     */
    public void storeRecordToKafka(String key, String value) {
        client.sendMessage(key, value);
        totalMessageCount++;
    }

    /**
     * Use breakpoint records filter already replayed records
     *
     * @param records the kafka breakpoint records
     * @return filterd records
     */
    public Collection<SinkRecord> readRecord(Collection<SinkRecord> records) {
        LOGGER.info("start sinkRecords size is {}", records.size());
        if (breakpointFilterList.isEmpty() && breakpointFilterMap.isEmpty()) {
            isGetBp = false;
        }
        if (Objects.isNull(bpRecordConsumer) && !isGetBp) {
            bpRecordConsumer = client.getReadConsumer();
        }
        long lastProcessedOffset = UNLIMITED_VALUE;
        Long endOffset = getEndOffsetOfDbBreakPointTopic(null, bpRecordConsumer);
        int recoveryAttempts = 0;
        boolean isTxnRecord = false;
        while (lastProcessedOffset < endOffset - 1 && !isGetBp) {
            if (recoveryAttempts > maxRecoveryAttempts) {
                LOGGER.warn("The breakpoint record couldn't be read. Consider to increase the value for "
                        + RECOVERY_POLL_ATTEMPTS.name());
                break;
            }
            Iterator<SinkRecord> iterator = records.iterator();
            SinkRecord firstSinkRecord = iterator.next();
            ConsumerRecords<String, String> breakpointRecords = bpRecordConsumer.poll(Duration.ofSeconds(1));
            ConsumerRecord<String, String> firstRecord = breakpointRecords.iterator().next();
            if (firstRecord.key().contains("-")) {
                isTxnRecord = true;
            }
            int numRecordsProcessed = 0;
            for (ConsumerRecord<String, String> record : breakpointRecords) {
                if (isTxnRecord) {
                    int index = record.key().lastIndexOf("-");
                    long bpBeginOffset = Long.parseLong(record.key().substring(0, index));
                    long bpEndOffset = Long.parseLong(record.key().substring(index + 1));
                    if (bpBeginOffset >= firstSinkRecord.kafkaOffset()) {
                        if (!breakpointFilterList.isEmpty() && Long.parseLong(breakpointFilterList
                                .getLast().split("-")[1]) == (bpBeginOffset - 1)) {
                            long preBeginOffset = Long.parseLong(breakpointFilterList.getLast().split("-")[0]);
                            breakpointFilterList.removeLast();
                            breakpointFilterList.add(preBeginOffset + "-" + bpEndOffset);
                        }
                        else {
                            breakpointFilterList.add(record.key());
                        }
                    }
                }
                else {
                    breakpointFilterMap.put(Long.parseLong(record.key()), Boolean.TRUE);
                }
                lastProcessedOffset = record.offset();
                ++numRecordsProcessed;
            }
            if (numRecordsProcessed == 0) {
                LOGGER.debug("No new records found in the breakpoint record; will retry");
                recoveryAttempts++;
            }
            else {
                LOGGER.debug("Processed {} records from breakpoint record", numRecordsProcessed);
            }
        }
        isGetBp = true;
        return filterByBp(records, isTxnRecord, endOffset);
    }

    private Collection<SinkRecord> filterByBp(Collection<SinkRecord> records, boolean isTxnRecord, Long endOffset) {
        // use breakpoint topic data filter already replay records
        Iterator<SinkRecord> iterator = records.iterator();
        Set<Long> replayedRecord = new HashSet<>();
        while (iterator.hasNext()) {
            SinkRecord sinkRecord = iterator.next();
            long kafkaOffset = sinkRecord.kafkaOffset();
            if (isTxnRecord) {
                for (String key : breakpointFilterList) {
                    int index = key.lastIndexOf("-");
                    long bpBeginOffset = Long.parseLong(key.substring(0, index));
                    long bpEndOffset = Long.parseLong(key.substring(index + 1));
                    if (kafkaOffset >= bpBeginOffset && kafkaOffset <= bpEndOffset) {
                        replayedRecord.add(sinkRecord.kafkaOffset());
                        replayedOffsets.offer(kafkaOffset);
                        iterator.remove();
                    }
                }
            }
            else {
                if (breakpointFilterMap.containsKey(kafkaOffset)) {
                    if (breakpointFilterMap.get(kafkaOffset)) {
                        replayedRecord.add(sinkRecord.kafkaOffset());
                        replayedOffsets.offer(kafkaOffset);
                        iterator.remove();
                    }
                }
            }
        }
        LOGGER.info("this offsets:{} is already successful replayed, skip the records",
                replayedRecord);
        LOGGER.info("filtered sinkRecords size is {}", records.size());
        if (records.size() > 0) {
            deleteBreakpoint(endOffset);
        }
        return records;
    }

    /**
     * Get breakpoint topic end offset
     *
     * @param previousEndOffset previous endOffset
     * @param bpRecordConsumer breakpoint consumer
     * @return Long endOffset
     */
    private Long getEndOffsetOfDbBreakPointTopic(Long previousEndOffset,
                                                 KafkaConsumer<String, String> bpRecordConsumer) {
        Map<TopicPartition, Long> offsets = bpRecordConsumer.endOffsets(
                Collections.singleton(client.getTopicPartition()));
        Long endOffset = offsets.entrySet().iterator().next().getValue();

        // The end offset should never change during recovery; doing this check here just as - a rather weak - attempt
        // to spot other connectors that share the same breakpoint topic accidentally
        if (previousEndOffset != null && !previousEndOffset.equals(endOffset)) {
            throw new IllegalStateException("Detected changed end offset of breakpoint topic (previous: "
                    + previousEndOffset + ", current: " + endOffset
                    + "). Make sure that the same breakpoint topic isn't shared by multiple connector instances.");
        }
        return endOffset;
    }

    /**
     * Delete the used breakpoint data
     *
     * @param endOffset the to delete endOffset
     */
    public void deleteBreakpoint(Long endOffset) {
        LOGGER.info("to delete endOffset is {}", endOffset);
        AdminClient kafkaAdminClient = client.createTopicClient();
        long lowWatermark = Long.MAX_VALUE;
        TopicPartition topicPartition = client.getTopicPartition();
        RecordsToDelete recordsToDelete = RecordsToDelete.beforeOffset(endOffset);
        LOGGER.info("recordsToDelete before offset is {}", recordsToDelete.beforeOffset());
        Map<TopicPartition, RecordsToDelete> recordsToDeleteMap = new HashMap<>();
        recordsToDeleteMap.put(topicPartition, recordsToDelete);
        DeleteRecordsResult result = kafkaAdminClient.deleteRecords(recordsToDeleteMap);
        Map<TopicPartition, KafkaFuture<DeletedRecords>> topicPartitionKafkaFutureMap = result.lowWatermarks();
        for (Map.Entry<TopicPartition, KafkaFuture<DeletedRecords>> entry : topicPartitionKafkaFutureMap.entrySet()) {
            try {
                lowWatermark = Math.min(entry.getValue().get().lowWatermark(), lowWatermark);
            }
            catch (InterruptedException | ExecutionException e) {
                LOGGER.error("Deleting the breakpoint kafka offset occur exception");
            }
        }
        LOGGER.info("deleted lowWatermark is {}", lowWatermark);
        kafkaAdminClient.close();
    }

    /**
     * Get store kafka queue size
     *
     * @return int store kafka queue size
     */
    public int getStoreKafkaQueueSize() {
        return storeToKafkaQueue.size();
    }

    /**
     * Find the can be deleted offset by committedOffset
     *
     * @param committedOffset replayed committed offset
     * @return Long the real breakpoint offset
     */
    public Long preDeleteOffset(Long committedOffset) {
        if (Objects.isNull(preDelConsumer)) {
            preDelConsumer = client.getPreDeleteConsumer();
        }
        boolean isTransaction = false;
        Long preDeleteOffset = null;
        long lastProcessedOffset = UNLIMITED_VALUE;
        Long endOffset = null;
        // find can delete offset
        do {
            endOffset = getEndOffsetOfDbBreakPointTopic(endOffset, preDelConsumer);
            ConsumerRecords<String, String> breakpointRecords = preDelConsumer.poll(Duration.ofSeconds(1));
            ConsumerRecord<String, String> firstRecord = breakpointRecords.iterator().next();
            if (firstRecord.key().contains("-")) {
                isTransaction = true;
            }
            for (ConsumerRecord<String, String> record : breakpointRecords) {
                if (isTransaction) {
                    int index = record.key().lastIndexOf("-");
                    Long bpEndOffset = Long.parseLong(record.key().substring(index + 1)) + 1;
                    if (bpEndOffset.equals(committedOffset)) {
                        preDeleteOffset = record.offset();
                        // use lastProcessedOffset break the loop
                        lastProcessedOffset = endOffset;
                        break;
                    }
                }
                else {
                    Long bpOffset = Long.parseLong(record.key()) + 1;
                    if (bpOffset.equals(committedOffset)) {
                        preDeleteOffset = record.offset();
                        // use lastProcessedOffset break the loop
                        lastProcessedOffset = endOffset;
                        break;
                    }
                }
                lastProcessedOffset = record.offset();
            }
        } while (lastProcessedOffset < endOffset - 1);
        return preDeleteOffset;
    }

    private void deleteBpByTimeTask() {
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                if (!toDeleteOffsets.isEmpty()) {
                    Long maxCommittedOffset = Collections.max(toDeleteOffsets);
                    toDeleteOffsets = toDeleteOffsets.stream().filter(offset -> offset > maxCommittedOffset)
                            .collect(Collectors.toList());
                    Long realBreakpointOffset = preDeleteOffset(maxCommittedOffset);
                    deleteBreakpoint(realBreakpointOffset);
                    LOGGER.info("In the period,delete the kafka data, kafka offset is {}", realBreakpointOffset + 1);
                }
            }
        };
        Timer timer = new Timer();
        timer.schedule(task, bpQueueTimeLimit * 1000 * 3600L,
                bpQueueTimeLimit * 1000 * 3600L);
    }

    private void deleteBpBySizeTask() {
        threadPool.execute(() -> {
            Thread.currentThread().setName("delete-breakpoint-thread");
            while (true) {
                // Additional thread monitor the queue size and delete them if the limit is exceeded
                if (totalMessageCount >= bpQueueSizeLimit) {
                    LOGGER.warn("the kafka offsets size is {}, it exceeded the limit", totalMessageCount);
                    Long maxCommittedOffset = Collections.max(toDeleteOffsets);
                    toDeleteOffsets = toDeleteOffsets.stream().filter(offset -> offset > maxCommittedOffset)
                            .collect(Collectors.toList());
                    Long realBreakpointOffset = preDeleteOffset(maxCommittedOffset);
                    deleteBreakpoint(realBreakpointOffset);
                    totalMessageCount = 0L;
                }
                try {
                    Thread.sleep(5000);
                }
                catch (InterruptedException e) {
                    LOGGER.warn("Receive interrupted exception while delete breakpoint records from kafka.");
                }
            }
        });
    }
}
