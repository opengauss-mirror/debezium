/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.full.migration.coordinator;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * QueueManager
 *
 * @since 2025-03-15
 */
public class QueueManager {
    private final BlockingQueue<Object> migrationQueue;

    private static QueueManager instance;

    private QueueManager() {
        migrationQueue = new LinkedBlockingQueue<>(10);
    }

    /**
     * getInstance
     *
     * @return QueueManager
     */
    public static synchronized QueueManager getInstance() {
        if (instance == null) {
            instance = new QueueManager();
        }
        return instance;
    }

    /**
     * addQueue
     *
     * @param object object
     * @throws InterruptedException InterruptedException
     */
    public synchronized void addQueue(Object object) throws InterruptedException {
        migrationQueue.put(object);
    }

    /**
     * takeQueue
     *
     * @return Object
     * @throws InterruptedException InterruptedException
     */
    public synchronized Object takeQueue() throws InterruptedException {
        return migrationQueue.take();
    }
}
