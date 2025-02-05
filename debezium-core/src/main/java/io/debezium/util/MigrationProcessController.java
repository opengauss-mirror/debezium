/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.util;

import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.SinkConnectorConfig;

/**
 * Description: Migration common feature class
 *
 * @author wang_zhengyuan
 * @since 2024-08-30
 */
public class MigrationProcessController {
    private static final Logger LOGGER = LoggerFactory.getLogger(MigrationProcessController.class);

    private int count = 0;
    private int pollIntervalSeconds;

    /**
     * Block the current thread
     *
     * @param millis long the block duration millions
     */
    public static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException exp) {
            LOGGER.warn("InterruptedException occurred while blocking the current thread, error message is: {}.",
                    exp.getMessage());
        }
    }

    /**
     * String is null or blank or ""
     *
     * @param value String the value
     *
     * @return true is value is null or blank or ""
     */
    public static boolean isNullOrBlank(String value) {
        return Strings.isNullOrBlank(value) || "\"\"".equals(value);
    }

    /**
     * initialize controller parameters
     *
     * @param config Sink connector config
     */
    public void initParameter(SinkConnectorConfig config) {
        pollIntervalSeconds = config.getMaxConsumerInterval() / 1000 - 60;
    }

    /**
     * wait connection alive
     *
     * @param isConnectionAlive AtomicBoolean the connection status
     */
    public void waitConnectionAlive(AtomicBoolean isConnectionAlive) {
        while (!isConnectionAlive.get()) {
            count++;
            MigrationProcessController.sleep(1000);
            if (count >= pollIntervalSeconds) {
                count = 0;
                LOGGER.warn("have wait {}}s, so skip the loop", pollIntervalSeconds);
                return;
            }
        }
    }
}
