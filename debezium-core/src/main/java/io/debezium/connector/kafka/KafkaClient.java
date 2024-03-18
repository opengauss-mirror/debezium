/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.kafka;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.base.Strings;
import io.debezium.config.Configuration;
import io.debezium.config.SinkConnectorConfig;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.util.Collect;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Description: Kafka client
 *
 * @author wangzhengyuan
 * @since 2023-08-12
 */
public class KafkaClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaClient.class);

    /**
     * The one and only partition of the breakpoint record topic.
     */
    public static final String CONFIGURATION_FIELD_PREFIX_STRING = "record.breakpoint.";

    private static final String CONFIG_TOPIC_PREFIX = "config_";
    private static final String DEFAULT_TOPIC_REPLICATION_FACTOR_PROP_NAME = "default.replication.factor";
    private static final short DEFAULT_TOPIC_REPLICATION_FACTOR = 1;
    private static final Duration KAFKA_QUERY_TIMEOUT = Duration.ofSeconds(3);
    private static final Integer PARTITION = 0;
    private static final short PARTITION_COUNT = (short) 1;
    private static final String CLEANUP_POLICY_NAME = "cleanup.policy";
    private static final String CLEANUP_POLICY_VALUE = "delete";
    private static final String RETENTION_MS_NAME = "retention.ms";
    private static final long RETENTION_MS_MAX = Long.MAX_VALUE;
    private static final String RETENTION_BYTES_NAME = "retention.bytes";

    private final String bootstrapServer;
    private final String topicName;
    private RelationalDatabaseConnectorConfig sourceConnectorConfig;
    private Configuration consumerConfig;
    private Configuration producerConfig;
    private volatile KafkaProducer<String, String> producer;
    private KafkaConsumer<String, String> consumer;

    /**
     * Constructor
     *
     * @param connectorConfig RelationalDatabaseConnectorConfig the connectorConfig
     */
    public KafkaClient(RelationalDatabaseConnectorConfig connectorConfig) {
        this.sourceConnectorConfig = connectorConfig;
        this.bootstrapServer = connectorConfig.kafkaBootstrapServer();
        this.topicName = CONFIG_TOPIC_PREFIX + connectorConfig.topic();
    }

    /**
     * Constructor
     *
     * @param bootstrapSever String the bootstrapSever
     */
    public KafkaClient(String bootstrapSever, String topic) {
        this.bootstrapServer = bootstrapSever;
        this.topicName = CONFIG_TOPIC_PREFIX + topic;
    }

    /**
     * Constructor
     *
     * @param connectorConfig SinkConnectorConfig the connector config
     * @param configPrefix String the config prefix
     */
    public KafkaClient(SinkConnectorConfig connectorConfig, String configPrefix) {
        this.bootstrapServer = connectorConfig.getBootstrapServers();
        this.topicName = connectorConfig.getBpTopic();
        String bpRecordName = configPrefix + UUID.randomUUID();
        this.consumerConfig = Configuration.create()
                .build()
                .subset(configPrefix + "consumer.", true).edit()
                .withDefault(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer)
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
        this.producerConfig = Configuration.create()
                .build()
                .subset(configPrefix + "producer.", true).edit()
                .withDefault(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer)
                .withDefault(ProducerConfig.ACKS_CONFIG, 1)
                .withDefault(ProducerConfig.RETRIES_CONFIG, 1) // may result in duplicate messages, but that's okay
                .withDefault(ProducerConfig.BATCH_SIZE_CONFIG, 1024 * 32) // 32KB
                .withDefault(ProducerConfig.LINGER_MS_CONFIG, 100)
                .withDefault(ProducerConfig.BUFFER_MEMORY_CONFIG, 1024 * 1024 * 64) // 64MB
                .withDefault(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                .withDefault(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                .withDefault(ProducerConfig.MAX_BLOCK_MS_CONFIG, 10_000) // wait at most this if we can't reach Kafka
                .build();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("BreakPointRecord Consumer config: {}", consumerConfig.withMaskedPasswords());
            LOGGER.debug("BreakPointRecord Producer config: {}", producerConfig.withMaskedPasswords());
        }
        this.producer = new KafkaProducer<>(this.producerConfig.asProperties());
    }

    public KafkaProducer<String, String> getProducer() {
        return producer;
    }

    public KafkaConsumer<String, String> getConsumer() {
        KafkaConsumer<String, String> defaultConsumer = new KafkaConsumer<>(consumerConfig.asProperties());
        defaultConsumer.subscribe(Collect.arrayListOf(topicName));
        return defaultConsumer;
    }

    /**
     * get read breakpoint info consumer
     *
     * @return kafkaConsumer kafkaConsumer
     */
    public KafkaConsumer<String, String> getReadConsumer() {
        this.consumerConfig = Configuration.copy(consumerConfig)
                .with(ConsumerConfig.GROUP_ID_CONFIG, CONFIGURATION_FIELD_PREFIX_STRING + UUID.randomUUID())
                .build();
        KafkaConsumer<String, String> readConsumer = new KafkaConsumer<>(this.consumerConfig.asProperties());
        readConsumer.subscribe(Collect.arrayListOf(topicName));
        return readConsumer;
    }

    /**
     * get pre breakpoint info consumer
     *
     * @return kafkaConsumer kafkaConsumer
     */
    public KafkaConsumer<String, String> getPreDeleteConsumer() {
        this.consumerConfig = Configuration.copy(consumerConfig)
                .with(ConsumerConfig.GROUP_ID_CONFIG, CONFIGURATION_FIELD_PREFIX_STRING + UUID.randomUUID())
                .build();
        KafkaConsumer<String, String> preDeleteConsumer = new KafkaConsumer<>(this.consumerConfig.asProperties());
        preDeleteConsumer.subscribe(Collect.arrayListOf(topicName));
        return preDeleteConsumer;
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
    public short getDefaultTopicReplicationFactor(AdminClient admin)
            throws InterruptedException, TimeoutException, ExecutionException {
        try {
            Config brokerConfig = getKafkaBrokerConfig(admin);
            String defaultReplicationFactorValue = brokerConfig.get(DEFAULT_TOPIC_REPLICATION_FACTOR_PROP_NAME).value();

            // Ensure that the default replication factor property was returned by the Admin Client
            if (defaultReplicationFactorValue != null) {
                return Short.parseShort(defaultReplicationFactorValue);
            }
        }
        catch (ExecutionException ex) {
            // ignore UnsupportedVersionException, e.g. due to older broker version
            if (!(ex.getCause() instanceof UnsupportedVersionException)) {
                throw ex;
            }
        }

        // Otherwise warn that no property was obtained and default it to 1 - users can increase this later if desired
        LOGGER.warn(
                "Unable to obtain the default replication factor from the brokers at {}. Setting value to {} instead.",
                bootstrapServer,
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
    private Config getKafkaBrokerConfig(AdminClient admin)
            throws ExecutionException, InterruptedException, TimeoutException {
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
     * Determine whether a topic exists
     *
     * @return boolean topic exists
     */
    public boolean isTopicExist() {
        try (KafkaConsumer<String, String> checkTopicConsumer = new KafkaConsumer<>(consumerConfig.asProperties())) {
            return checkTopicConsumer.listTopics().containsKey(topicName);
        }
    }

    /**
     * Gets kafka topic partition
     *
     * @return TopicPartition the topic partition
     */
    public TopicPartition getTopicPartition() {
        return new TopicPartition(topicName, PARTITION);
    }

    /**
     * Creates topic client
     *
     * @return AdminClient the topic client
     */
    public AdminClient createTopicClient() {
        return AdminClient.create(this.producerConfig.asProperties());
    }

    /**
     * Sends break point message to kafka
     *
     * @param key String the key of break point message
     * @param value String the value of break point message
     * @return Future<RecordMetadata> the future
     */
    public Future<RecordMetadata> sendMessage(String key, String value) {
        ProducerRecord<String, String> produced = new ProducerRecord<>(topicName, PARTITION, key, value);
        return producer.send(produced);
    }

    /**
     * Initialize break point storage in kafka
     *
     * @param unlimitedValue Long the unlimited value
     */
    public void initializeStorage(Long unlimitedValue) {
        if (!isTopicExist()) {
            try (AdminClient admin = createTopicClient()) {
                // Find default replication factor
                final short replicationFactor = getDefaultTopicReplicationFactor(admin);
                // Create topic
                final NewTopic topic = new NewTopic(this.topicName, PARTITION_COUNT, replicationFactor);
                topic.configs(Collect.hashMapOf(CLEANUP_POLICY_NAME, CLEANUP_POLICY_VALUE,
                        RETENTION_MS_NAME, Long.toString(RETENTION_MS_MAX), RETENTION_BYTES_NAME,
                        Long.toString(unlimitedValue)));
                admin.createTopics(Collections.singleton(topic));

                LOGGER.info("Breakpoint record topic '{}' created", topic);
            }
            catch (InterruptedException | TimeoutException | ExecutionException e) {
                throw new ConnectException("Creation of breakpoint record topic failed, "
                        + "please create the topic manually", e);
            }
        }
    }

    /**
     * Sends config to kafka
     */
    public void sendConfigToKafka() {
        Properties producerProperties = getProducerProperties();
        AdminClient client = createTopicClient(producerProperties);
        Properties consumerProperties = getConsumerProperties(bootstrapServer);
        refreshTopic(client, consumerProperties, topicName);
        produceAndSend(producerProperties, topicName, String.valueOf(sourceConnectorConfig.getConnectorConfigList()));
        String message = readConfig();
        if (Strings.isNullOrEmpty(message)) {
            sendConfigToKafka();
        } else {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Send source config successfully, the result is:" + System.lineSeparator()
                        + message);
            }
        }
        client.close();
    }

    /**
     * Read source config
     *
     * @return String the source config list
     */
    public String readSourceConfig() {
        String result = readConfig();
        while (Strings.isNullOrEmpty(result)) {
            try {
                LOGGER.info("Wait for source config ...");
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                LOGGER.error("Interrupted exception occurred", e);
            }
            result = readConfig();
        }
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Read source config successfully, the result is:" + System.lineSeparator() + result);
        }
        return result;
    }

    private String readConfig() {
        Properties consumerProperties = getConsumerProperties(bootstrapServer);

        // Set whether to automatically submit after pulling information
        // Kafka records whether the current app has already obtained this information
        // true: auto commit
        // false: manual commit
        consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        // earliest: pull from the first piece of data
        // latest: pull the latest data
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        KafkaConsumer<String, String> configConsumer = new KafkaConsumer<>(consumerProperties);
        configConsumer.subscribe(Collections.singleton(topicName));
        ConsumerRecords<String, String> records = configConsumer.poll(Duration.ofMillis(1000));
        String value = "";
        if (!records.isEmpty()) {
            value = records.iterator().next().value();
        }
        if(!Strings.isNullOrEmpty(value)){
            configConsumer.commitAsync();
            configConsumer.close();
        }
        return value;
    }

    private void produceAndSend(Properties producerProperties, String topic, String value) {
        Producer<String, String> configProducer = new KafkaProducer<>(producerProperties);
        Future<RecordMetadata> future = configProducer.send(new ProducerRecord<>(topic, String.valueOf(value)));
        configProducer.flush();
        try {
            future.get();
        } catch (InterruptedException exp) {
            LOGGER.warn("receive interrupted exception when send config record.");
        } catch (ExecutionException exp) {
            LOGGER.warn("receive execution exception when send config record.");
        }
        configProducer.close();
    }

    private void refreshTopic(AdminClient client, Properties consumerProperties, String topic) {
        short rs = 1;
        NewTopic newTopic = new NewTopic(topic, 1, rs);
        KafkaConsumer<String, String> checkTopicConsumer = new KafkaConsumer<>(consumerProperties);
        if (checkTopicConsumer.listTopics().containsKey(topic)) {
            client.deleteTopics(Collections.singletonList(topic));
        }
        checkTopicConsumer.close();
        client.createTopics(Arrays.asList(newTopic));
    }

    private Properties getConsumerProperties(String bootstrapServer) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        return properties;
    }

    private AdminClient createTopicClient(Properties properties) {
        return KafkaAdminClient.create(properties);
    }

    private Properties getProducerProperties() {
        Properties properties = new Properties();
        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.put("key.serializer", StringSerializer.class);
        properties.put("value.serializer", StringSerializer.class);
        return properties;
    }
}
