/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */

package io.debezium.log4j;

import com.alibaba.fastjson.JSON;
import io.debezium.enums.ErrorCode;
import io.debezium.log4j.appender.KafkaAppender;
import io.debezium.log4j.filter.RegexFilter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Level;
import org.apache.log4j.PatternLayout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * appender loader
 *
 * @since 2024/12/11
 */
public class AppenderLoader {
    private static final Logger LOGGER = LoggerFactory.getLogger(AppenderLoader.class);
    private static final String LAYOUT = "%d{yyyy-MM-dd HH:mm:ss.SSS} [%t] %p %c:(%L) - %m%n";
    private static final String REGEX = "<CODE:\\d{4}> [\\s\\S]+";

    private static volatile boolean isAddAppender = false;
    private static boolean isAlertLogCollectionEnabled = false;
    private static String kafkaServer;
    private static String kafkaKey;
    private static String kafkaTopic;

    static {
        if (getSystemProperty("enable.alert.log.collection").equals("true")) {
            isAlertLogCollectionEnabled = true;
            kafkaServer = getSystemProperty("kafka.bootstrapServers");
            kafkaKey = getSystemProperty("kafka.key");
            kafkaTopic = getSystemProperty("kafka.topic");
        }
    }

    /**
     * load kafka appender and register error code
     */
    public static void loadAppender() {
        if (!isAddAppender) {
            synchronized (AppenderLoader.class) {
                if (!isAddAppender) {
                    if (isAlertLogCollectionEnabled) {
                        addAppender();
                        registerErrorCode();
                    }
                    isAddAppender = true;
                }
            }
        }
    }

    private static void addAppender() {
        try {
            if (kafkaServer.isEmpty() || kafkaKey.isEmpty() || kafkaTopic.isEmpty()) {
                LOGGER.error("Kafka configuration is missing. "
                        + "Please check kafka.bootstrapServers, kafka.key, and kafka.topic properties.");
                return;
            }

            KafkaAppender kafkaAppender = getKafkaAppender();

            org.apache.log4j.Logger rootLogger = org.apache.log4j.Logger.getRootLogger();
            rootLogger.addAppender(kafkaAppender);

            LOGGER.info("KafkaAppender has been successfully added to the rootLogger.");
        } catch (Exception e) {
            LOGGER.error("Failed to add KafkaAppender due to an exception.", e);
        }
    }

    private static KafkaAppender getKafkaAppender() {
        KafkaAppender kafkaAppender = new KafkaAppender();
        kafkaAppender.setBootstrapServers(kafkaServer);
        kafkaAppender.setKey(kafkaKey);
        kafkaAppender.setTopic(kafkaTopic);

        PatternLayout patternLayout = new PatternLayout(LAYOUT);
        kafkaAppender.setLayout(patternLayout);
        kafkaAppender.setThreshold(Level.ERROR);

        RegexFilter regexFilter = getRegexFilter();
        kafkaAppender.addFilter(regexFilter);
        kafkaAppender.activateOptions();
        return kafkaAppender;
    }

    private static void registerErrorCode() {
        Map<Integer, String> codeCauseCnMap = new HashMap<>();
        Map<Integer, String> codeCauseEnMap = new HashMap<>();
        ErrorCode[] errorCodes = ErrorCode.values();
        for (ErrorCode errorCode : errorCodes) {
            codeCauseCnMap.put(errorCode.getCode(), errorCode.getCauseCn());
            codeCauseEnMap.put(errorCode.getCode(), errorCode.getCauseEn());
        }
        String codeCauseCnString = JSON.toJSONString(codeCauseCnMap);
        String codeCauseEnString = JSON.toJSONString(codeCauseEnMap);

        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Producer<String, String> producer = new KafkaProducer<>(config);
        ProducerRecord<String, String> record = new ProducerRecord<>(kafkaTopic, "debezium",
                "<CODE:causeCn>" + codeCauseCnString + "<CODE:causeEn>" + codeCauseEnString);
        producer.send(record);
        producer.close();
    }

    private static String getSystemProperty(String propertyName) {
        String propertyValue = System.getProperty(propertyName);
        if (propertyValue == null || propertyValue.trim().isEmpty()) {
            LOGGER.error("Required property " + propertyName + " is missing or empty.");
            return "";
        }
        return propertyValue;
    }

    private static RegexFilter getRegexFilter() {
        RegexFilter regexFilter = new RegexFilter();
        regexFilter.setRegex(REGEX);
        return regexFilter;
    }
}
