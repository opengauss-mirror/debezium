/*
 * Copyright 2026 Huawei Technologies Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
     * tableAutoIncrementQueue
     */
    public static final String TABLE_AUTO_INCREMENT_QUEUE = "tableAutoIncrementQueue";

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
