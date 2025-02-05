/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package io.debezium.pipeline.spi;

import io.debezium.migration.NotifyEvent;

/**
 * EOF message emitter
 *
 * @author jianghongbo
 * @since 2025/2/6
 */
public interface NotifyEventEmitter {
    /**
     * emit EOF message to kafka
     *
     * @param receiver Receiver
     * @throws InterruptedException emit message to kafka may throw
     */
    void emitNotifyEvent(NotifyEventEmitter.Receiver receiver) throws InterruptedException;

    /**
     * EOF message receiver
     *
     * @author jianghongbo
     * @since 2025/2/6
     */
    public interface Receiver {
        /**
         * queue receive EOF message
         *
         * @param event NotifyEvent
         * @throws InterruptedException emit message to kafka may throw
         */
        void notifyEvent(NotifyEvent event) throws InterruptedException;
    }
}
