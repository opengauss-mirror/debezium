/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium;

import io.debezium.enums.ErrorCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Description: Thread exception handler
 *
 * @author wang_zhengyuan
 * @since 2024-12-21
 */
public class ThreadExceptionHandler implements Thread.UncaughtExceptionHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(ThreadExceptionHandler.class);

    @Override
    public void uncaughtException(Thread t, Throwable e) {
        LOGGER.error("{}Uncaught exception occurred in thread {}, error message is: ", ErrorCode.UNKNOWN, t.getName(), e);
        throw new RuntimeException(e);
    }
}
