/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package io.debezium.pipeline.spi;

import io.debezium.migration.ObjectChangeEvent;

/**
 * Description: ObjectChangeEventEmitter
 *
 * @author jianghongbo
 * @since 2024/11/25
 */
public interface ObjectChangeEventEmitter {
    /**
     * emit object info to kafka
     *
     * @param receiver Receiver
     * @throws InterruptedException this method may interrupt
     */
    void emitObjectChangeEvent(Receiver receiver) throws InterruptedException;

    /**
     * Description: object message receiver
     *
     * @author jianghongbo
     * @since 2025/2/6
     */
    public interface Receiver {
        /**
         * queue receive object message
         *
         * @param event ObjectChangeEvent
         * @throws InterruptedException this method may interrupt
         */
        void objectChangeEvent(ObjectChangeEvent event) throws InterruptedException;
    }
}
