/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.breakpoint;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.DeleteRecordsResult;
import org.apache.kafka.clients.admin.DeletedRecords;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.Queues;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.util.Collect;

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
     * The breakpoint topic
     */
    public static final Field TOPIC = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "kafka.topic")
            .withDisplayName("Database breakpoint topic name")
            .withType(ConfigDef.Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 32))
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("The name of the topic for the database breakpoint record");

    /**
     * The breakpoint kafka bootstrap server
     */
    public static final Field BOOTSTRAP_SERVERS = Field.create(CONFIGURATION_FIELD_PREFIX_STRING
            + "kafka.bootstrap.servers")
            .withDisplayName("Kafka broker addresses")
            .withType(ConfigDef.Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 31))
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("A list of host/port pairs that the connector will use for establishing the initial "
                    + "connection to the Kafka cluster for retrieving database schema history previously stored "
                    + "by the connector. This should point to the same Kafka cluster used by the Kafka Connect "
                    + "process.");

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

    private static final Integer PARTITION = 0;
    private static final Long UNLIMITED_VALUE = -1L;
    private static final short PARTITION_COUNT = (short) 1;
    private static final String CLEANUP_POLICY_NAME = "cleanup.policy";
    private static final String CLEANUP_POLICY_VALUE = "delete";
    private static final String RETENTION_MS_NAME = "retention.ms";
    private static final long RETENTION_MS_MAX = Long.MAX_VALUE;
    private static final long RETENTION_MS_MIN = Duration.of(5 * 365, ChronoUnit.DAYS).toMillis(); // 5 years
    private static final String RETENTION_BYTES_NAME = "retention.bytes";

    /**
     * The name of broker property defining default replication factor for topics without the explicit setting.
     *
     * @see kafka.server.KafkaConfig.DefaultReplicationFactorProp
     */
    private static final String DEFAULT_TOPIC_REPLICATION_FACTOR_PROP_NAME = "default.replication.factor";

    /**
     * The default replication factor for the breakpoint topic which is used in case
     * the value couldn't be retrieved from the broker.
     */
    private static final short DEFAULT_TOPIC_REPLICATION_FACTOR = 1;
    private static final Duration KAFKA_QUERY_TIMEOUT = Duration.ofSeconds(3);
    private static final String CONSUMER_PREFIX = CONFIGURATION_FIELD_PREFIX_STRING + "consumer.";
    private static final String PRODUCER_PREFIX = CONFIGURATION_FIELD_PREFIX_STRING + "producer.";

    private Configuration consumerConfig;
    private Configuration producerConfig;
    private volatile KafkaProducer<String, String> producer;
    private KafkaConsumer<String, String> bpRecordConsumer;
    private Set<String> breakpointFilterSet = new HashSet<>();
    private Map<Long, Boolean> breakpointFilterMap = new HashMap<>();
    private BlockingQueue<BreakPointInfo> storeToKafkaQueue = new LinkedBlockingQueue<>();
    private final ThreadPoolExecutor threadPool = new ThreadPoolExecutor(2, 2, 100,
            TimeUnit.SECONDS, new LinkedBlockingQueue<>(2));
    private List<Long> toDeleteOffsets = new ArrayList<>();
    private String bpRecordTopicName;
    private Duration pollInterval;
    private int maxRecoveryAttempts;
    private int bpQueueTimeLimit;
    private int bpQueueSizeLimit;
    private Long totalMessageCount = 0L;
    private PriorityBlockingQueue<Long> replayedOffsets;
    private boolean isGetBp;
    private Long breakpointEndOffset = UNLIMITED_VALUE;

    /**
     * configure the breakpoint properties
     *
     * @param config Configuration config
     */
    public BreakPointRecord(Configuration config) {
        this.bpRecordTopicName = config.getString(TOPIC);
        this.replayedOffsets = new PriorityBlockingQueue<>();
        this.pollInterval = Duration.ofMillis(config.getInteger(RECOVERY_POLL_INTERVAL_MS));
        this.maxRecoveryAttempts = config.getInteger(RECOVERY_POLL_ATTEMPTS);
        String bootstrapServers = config.getString(BOOTSTRAP_SERVERS);
        String bpRecordName = config.getString(CONFIGURATION_FIELD_PREFIX_STRING + "name",
                UUID.randomUUID().toString());
        this.consumerConfig = config.subset(CONSUMER_PREFIX, true).edit()
                .withDefault(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
                .withDefault(ConsumerConfig.GROUP_ID_CONFIG, bpRecordName)
                .withDefault(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1) // get even smallest message
                .withDefault(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
                .withDefault(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000) // readjusted since 0.10.1.0
                .withDefault(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 30000)
                .withDefault(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                        OffsetResetStrategy.EARLIEST.toString().toLowerCase(Locale.ROOT))
                .withDefault(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                .withDefault(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                .build();
        this.producerConfig = config.subset(PRODUCER_PREFIX, true).edit()
                .withDefault(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
                .withDefault(ProducerConfig.ACKS_CONFIG, 1)
                .withDefault(ProducerConfig.RETRIES_CONFIG, 1) // may result in duplicate messages, but that's okay
                .withDefault(ProducerConfig.BATCH_SIZE_CONFIG, 1024 * 32) // 32KB
                .withDefault(ProducerConfig.LINGER_MS_CONFIG, 100)
                .withDefault(ProducerConfig.BUFFER_MEMORY_CONFIG, 1024 * 1024 * 64) // 64MB
                .withDefault(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                .withDefault(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                .withDefault(ProducerConfig.MAX_BLOCK_MS_CONFIG, 10_000) // wait at most this if we can't reach Kafka
                .build();
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("BreakPointRecord Consumer config: {}", consumerConfig.withMaskedPasswords());
            LOGGER.info("BreakPointRecord Producer config: {}", producerConfig.withMaskedPasswords());
        }
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
     * Sets the bpQueueTimeLimit.
     *
     * @param bpQueueTimeLimit the store breakpoint cleanup time limit
     */
    public void setBpQueueTimeLimit(int bpQueueTimeLimit) {
        this.bpQueueTimeLimit = bpQueueTimeLimit;
    }

    /**
     * Sets the bpQueueSizeLimit.
     *
     * @param bpQueueSizeLimit the store breakpoint cleanup size limit
     */
    public void setBpQueueSizeLimit(int bpQueueSizeLimit) {
        this.bpQueueSizeLimit = bpQueueSizeLimit;
    }

    /**
     * Start the record breakpoint info.
     */
    public synchronized void start() {
        storeToKafkaThread();
        deleteBpByTimeTask();
        deleteBpBySizeTask();
        if (this.producer == null) {
            this.producer = new KafkaProducer<>(this.producerConfig.asProperties());
        }
    }

    /**
     * build consumer config property name
     *
     * @param kafkaConsumerPropertyName the consumer property name
     * @return consumer prefix and kafka consumer property
     */
    public static String consumerConfigPropertyName(String kafkaConsumerPropertyName) {
        return CONSUMER_PREFIX + kafkaConsumerPropertyName;
    }

    /**
     * initialize breakpoint storage in kafka
     */
    public void initializeStorage() {
        try (AdminClient admin = AdminClient.create(this.producerConfig.asProperties())) {
            // Find default replication factor
            final short replicationFactor = getDefaultTopicReplicationFactor(admin);
            // Create topic
            final NewTopic topic = new NewTopic(bpRecordTopicName, PARTITION_COUNT, replicationFactor);
            topic.configs(Collect.hashMapOf(CLEANUP_POLICY_NAME, CLEANUP_POLICY_VALUE,
                    RETENTION_MS_NAME, Long.toString(RETENTION_MS_MAX), RETENTION_BYTES_NAME,
                    Long.toString(UNLIMITED_VALUE)));
            admin.createTopics(Collections.singleton(topic));

            LOGGER.info("Breakpoint record topic '{}' created", topic);
        } catch (InterruptedException | TimeoutException | ExecutionException e) {
            throw new ConnectException("Creation of breakpoint record topic failed, "
                    + "please create the topic manually", e);
        }
    }

    /**
     * Get default topic replication factor
     *
     * @param admin kafka admin
     * @return default topic replication factor
     * @throws InterruptedException throw InterruptedException
     * @throws TimeoutException throw TimeoutException
     * @throws ExecutionException throw ExecutionException
     */
    private short getDefaultTopicReplicationFactor(AdminClient admin) throws
            InterruptedException, TimeoutException, ExecutionException {
        try {
            Config brokerConfig = getKafkaBrokerConfig(admin);
            String defaultReplicationFactorValue = brokerConfig.get(DEFAULT_TOPIC_REPLICATION_FACTOR_PROP_NAME).value();

            // Ensure that the default replication factor property was returned by the Admin Client
            if (defaultReplicationFactorValue != null) {
                return Short.parseShort(defaultReplicationFactorValue);
            }
        } catch (ExecutionException ex) {
            // ignore UnsupportedVersionException, e.g. due to older broker version
            if (!(ex.getCause() instanceof UnsupportedVersionException)) {
                throw ex;
            }
        }

        // Otherwise warn that no property was obtained and default it to 1 - users can increase this later if desired
        LOGGER.warn(
                "Unable to obtain the default replication factor from the brokers at {}. Setting value to {} instead.",
                producerConfig.getString(BOOTSTRAP_SERVERS),
                DEFAULT_TOPIC_REPLICATION_FACTOR);

        return DEFAULT_TOPIC_REPLICATION_FACTOR;
    }

    /**
     * Get kafka broker config
     *
     * @param admin kafka admin
     * @return Config kafka broker config
     * @throws ExecutionException throw ExecutionException
     * @throws InterruptedException throw InterruptedException
     * @throws TimeoutException throw TimeoutException
     */
    private Config getKafkaBrokerConfig(AdminClient admin) throws
            ExecutionException, InterruptedException, TimeoutException {
        final Collection<Node> nodes = admin.describeCluster().nodes()
                .get(KAFKA_QUERY_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        if (nodes.isEmpty()) {
            throw new ConnectException("No brokers available to obtain default settings");
        }
        String nodeId = nodes.iterator().next().idString();
        Set<ConfigResource> resources = Collections.singleton(new ConfigResource(ConfigResource.Type.BROKER, nodeId));
        final Map<ConfigResource, Config> configs = admin.describeConfigs(resources).all().get(
                KAFKA_QUERY_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        if (configs.isEmpty()) {
            throw new ConnectException("No configs have been received");
        }
        return configs.values().iterator().next();
    }

    /**
     * Determine if there is a breakpoint condition
     *
     * @param records source connector collected data
     * @return boolean breakpoint situation
     */
    public boolean isExists(Collection<SinkRecord> records) {
        if (isTopicExist() && breakpointEndOffset.equals(UNLIMITED_VALUE)) {
            KafkaConsumer<String, String> breakpointConsumer = new KafkaConsumer<>(consumerConfig.asProperties());
            breakpointConsumer.subscribe(Collect.arrayListOf(bpRecordTopicName));
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
                LOGGER.debug("End offset of breakpoint topic is {}", endOffset);
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
                            } else {
                                long recordEndOffset = Long.parseLong(record.key());
                                breakpointEndOffset = Math.max(breakpointEndOffset, recordEndOffset);
                            }
                        }
                        lastProcessedOffset = record.offset();
                        ++numRecordsProcessed;
                    } catch (NumberFormatException e) {
                        LOGGER.error("NumberFormat exception while processing record '{}'", record, e);
                    }
                }
                if (numRecordsProcessed == 0) {
                    LOGGER.debug("No new records found in the breakpoint record; will retry");
                    recoveryAttempts++;
                } else {
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
        LOGGER.debug("Storing Breakpoint record into breakpoint topic: {}", record.toString());
        String key = "";
        String value = "";
        if (isTransaction) {
            key = record.getBeginOffset() + "-" + record.getEndOffset();
            value = "beginOffset=" + record.getBeginOffset()
                    + ", endOffset=" + record.getEndOffset()
                    + ", gtid=" + record.getGtid()
                    + ", timestamp=" + record.getTimeStamp();
        } else {
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
        storeToKafkaQueue.add(
                new BreakPointInfo()
                        .setKey(key)
                        .setValue(value));
    }

    private void storeToKafkaThread() {
        threadPool.execute(() -> {
            while (true) {
                List<BreakPointInfo> toStoreList = new ArrayList<>();
                try {
                    Queues.drain(storeToKafkaQueue, toStoreList, 3000, 100, TimeUnit.MILLISECONDS);
                    for (BreakPointInfo breakPointInfo : toStoreList) {
                        storeRecordToKafka(breakPointInfo.getKey(), breakPointInfo.getValue());
                        this.producer.flush();
                    }
                } catch (InterruptedException e) {
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
        ProducerRecord<String, String> produced = new ProducerRecord<>(bpRecordTopicName, PARTITION,
                key, value);
        Future<RecordMetadata> future = this.producer.send(produced);
        // Flush and then wait ...
        RecordMetadata metadata = null; // block forever since we have to be sure this gets recorded
        try {
            metadata = future.get();
            if (metadata != null) {
                LOGGER.debug("Stored record in topic '{}' partition {} at offset {} ",
                        metadata.topic(), metadata.partition(), metadata.offset());
            }
            totalMessageCount++;
        } catch (InterruptedException e) {
            LOGGER.trace("Interrupted before record was written into kafka file");
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            LOGGER.error("");
        }
    }

    /**
     * Determine whether a topic exists
     *
     * @return boolean topic exists
     */
    public boolean isTopicExist() {
        try (KafkaConsumer<String, String> checkTopicConsumer = new KafkaConsumer<>(consumerConfig.asProperties())) {
            return checkTopicConsumer.listTopics().containsKey(bpRecordTopicName);
        }
    }

    /**
     * Use breakpoint records filter already replayed records
     *
     * @param records the kafka breakpoint records
     * @return filterd records
     */
    public Collection<SinkRecord> readRecord(Collection<SinkRecord> records) {
        LOGGER.info("start sinkRecords size is {}", records.size());
        if (breakpointFilterSet.isEmpty() && breakpointFilterMap.isEmpty()) {
            isGetBp = false;
        }
        if (this.bpRecordConsumer == null && !isGetBp) {
            this.bpRecordConsumer = new KafkaConsumer<>(consumerConfig.asProperties());
            this.bpRecordConsumer.subscribe(Collect.arrayListOf(bpRecordTopicName));
        }
        long lastProcessedOffset = UNLIMITED_VALUE;
        Long endOffset = null;
        int recoveryAttempts = 0;
        boolean isTxnRecord = false;
        do {
            if (recoveryAttempts > maxRecoveryAttempts) {
                LOGGER.warn("The breakpoint record couldn't be read. Consider to increase the value for "
                        + RECOVERY_POLL_ATTEMPTS.name());
                break;
            }
            endOffset = getEndOffsetOfDbBreakPointTopic(endOffset, bpRecordConsumer);
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
                    if (bpBeginOffset >= firstSinkRecord.kafkaOffset()) {
                        breakpointFilterSet.add(record.key());
                    }
                } else {
                    breakpointFilterMap.put(Long.parseLong(record.key()), Boolean.TRUE);
                }
                lastProcessedOffset = record.offset();
                ++numRecordsProcessed;
            }
            if (numRecordsProcessed == 0) {
                LOGGER.debug("No new records found in the breakpoint record; will retry");
                recoveryAttempts++;
            } else {
                LOGGER.debug("Processed {} records from breakpoint record", numRecordsProcessed);
            }
        } while (lastProcessedOffset < endOffset - 1 && !isGetBp);
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
                for (String key : breakpointFilterSet) {
                    int index = key.lastIndexOf("-");
                    long bpBeginOffset = Long.parseLong(key.substring(0, index));
                    long bpEndOffset = Long.parseLong(key.substring(index + 1));
                    if (kafkaOffset >= bpBeginOffset && kafkaOffset <= bpEndOffset) {
                        replayedRecord.add(sinkRecord.kafkaOffset());
                        replayedOffsets.offer(kafkaOffset);
                        iterator.remove();
                    }
                }
            } else {
                if (breakpointFilterMap.containsKey(kafkaOffset)) {
                    if (breakpointFilterMap.get(kafkaOffset)) {
                        replayedRecord.add(sinkRecord.kafkaOffset());
                        replayedOffsets.offer(kafkaOffset);
                        iterator.remove();
                    }
                }
            }
        }
        LOGGER.info("this offsets:{} is already successful replayed,skip the records",
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
                Collections.singleton(new TopicPartition(bpRecordTopicName, PARTITION)));
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
        AdminClient kafkaAdminClient = AdminClient.create(this.producerConfig.asProperties());
        long lowWatermark = Long.MAX_VALUE;
        TopicPartition topicPartition = new TopicPartition(bpRecordTopicName, PARTITION);
        RecordsToDelete recordsToDelete = RecordsToDelete.beforeOffset(endOffset);
        LOGGER.info("recordsToDelete before offset is {}", recordsToDelete.beforeOffset());
        Map<TopicPartition, RecordsToDelete> recordsToDeleteMap = new HashMap<>();
        recordsToDeleteMap.put(topicPartition, recordsToDelete);
        DeleteRecordsResult result = kafkaAdminClient.deleteRecords(recordsToDeleteMap);
        Map<TopicPartition, KafkaFuture<DeletedRecords>> topicPartitionKafkaFutureMap = result.lowWatermarks();
        for (Map.Entry<TopicPartition, KafkaFuture<DeletedRecords>> entry : topicPartitionKafkaFutureMap.entrySet()) {
            try {
                lowWatermark = Math.min(entry.getValue().get().lowWatermark(), lowWatermark);
            } catch (InterruptedException | ExecutionException e) {
                LOGGER.error("Deleting the breakpoint kafka offset occur exception");
            }
        }
        LOGGER.info("deleted lowWatermark is {}", lowWatermark);
        kafkaAdminClient.close();
    }

    /**
     * Find the can be deleted offset by committedOffset
     *
     * @param committedOffset replayed committed offset
     * @return Long the real breakpoint offset
     */
    public Long preDeleteOffset(Long committedOffset) {
        KafkaConsumer<String, String> getDeleteOffsetConsumer = new KafkaConsumer<>(consumerConfig.asProperties());
        getDeleteOffsetConsumer.subscribe(Collect.arrayListOf(bpRecordTopicName));
        boolean isTransaction = false;
        Long preDeleteOffset = null;
        long lastProcessedOffset = UNLIMITED_VALUE;
        Long endOffset = null;
        // find can delete offset
        do {
            endOffset = getEndOffsetOfDbBreakPointTopic(endOffset, getDeleteOffsetConsumer);
            ConsumerRecords<String, String> breakpointRecords = getDeleteOffsetConsumer.poll(Duration.ofSeconds(1));
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
                } else {
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
            while (true) {
                // Additional thread monitor the queue size and delete them if the limit is exceeded
                if (totalMessageCount >= bpQueueSizeLimit) {
                    LOGGER.warn("the kafka offsets size is {},it exceeded the limit", totalMessageCount);
                    Long maxCommittedOffset = Collections.max(toDeleteOffsets);
                    Long realBreakpointOffset = preDeleteOffset(maxCommittedOffset);
                    deleteBreakpoint(realBreakpointOffset);
                    totalMessageCount = 0L;
                }
            }
        });
    }
}
