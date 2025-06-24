/*
 * Copyright (c) 2025-2025 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *           http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.full.migration.coordinator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * QueueManager
 *
 * @since 2025-04-18
 */
public class QueueManager {
    /**
     * tableQueue
     */
    public static final String TABLE_QUEUE = "tableQueue";

    /**
     * typesQueue
     */
    public static final String TYPES_QUEUE = "typesQueue";

    /**
     * sourceTableMetaQueue
     */
    public static final String SOURCE_TABLE_META_QUEUE = "sourceTableMetaQueue";

    /**
     * targetTableMetaQueue
     */
    public static final String TARGET_TABLE_META_QUEUE = "targetTableMetaQueue";

    /**
     * tableDataQueue
     */
    public static final String TABLE_DATA_QUEUE = "tableDataQueue";

    /**
     * tablePrimaryKeyQueue
     */
    public static final String TABLE_PRIMARY_KEY_QUEUE = "tablePrimaryKeyQueue";

    /**
     * tableForeignKeyQueue
     */
    public static final String TABLE_FOREIGN_KEY_QUEUE = "tableForeignKeyQueue";

    /**
     * tableIndexQueue
     */
    public static final String TABLE_INDEX_QUEUE = "tableIndexQueue";

    /**
     * tableConstraintQueue
     */
    public static final String TABLE_CONSTRAINT_QUEUE = "tableConstraintQueue";

    /**
     * objectQueue
     */
    public static final String OBJECT_QUEUE = "objectQueue";

    private static final Logger LOGGER = LoggerFactory.getLogger(QueueManager.class);

    private static QueueManager instance;

    private final Map<String, QueueWrapper> queueMap;

    private QueueManager() {
        this.queueMap = new ConcurrentHashMap<>();
    }

    /**
     * getInstance
     *
     * @return QueueManager instance
     */
    public static synchronized QueueManager getInstance() {
        if (instance == null) {
            instance = new QueueManager();
        }
        return instance;
    }

    private static class QueueWrapper {
        BlockingQueue<Object> queue;
        AtomicBoolean isReadFinished;

        QueueWrapper(int capacity) {
            this.queue = new LinkedBlockingQueue<>(capacity);
            this.isReadFinished = new AtomicBoolean(false);
        }
    }

    private QueueWrapper getQueueWrapper(String queueName) {
        return queueMap.computeIfAbsent(queueName, k -> new QueueWrapper(100));
    }

    /**
     * putToQueue
     *
     * @param queueName queueName
     * @param object object
     */
    public void putToQueue(String queueName, Object object) {
        try {
            getQueueWrapper(queueName).queue.put(object);
        } catch (InterruptedException e) {
            LOGGER.error("put object to queue has occurred an exception, error message:{}", e.getMessage());
        }
    }

    /**
     * pollQueue
     *
     * @param queueName queueName
     * @return Object
     */
    public Object pollQueue(String queueName) {
        try {
            return getQueueWrapper(queueName).queue.poll(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOGGER.error("take object from queue has occurred an exception, error message:{}", e.getMessage());
        }
        return null;
    }

    /**
     * isQueuePollEnd
     *
     * @param queueName queueName
     * @return isQueuePollEnd
     */
    public boolean isQueuePollEnd(String queueName) {
        QueueWrapper queueWrapper = getQueueWrapper(queueName);
        return queueWrapper.isReadFinished.get() && queueWrapper.queue.isEmpty();
    }

    /**
     * setReadFinished
     *
     * @param queueName queueName
     * @param isFinished isFinished
     */
    public void setReadFinished(String queueName, boolean isFinished) {
        getQueueWrapper(queueName).isReadFinished.set(isFinished);
    }
}
