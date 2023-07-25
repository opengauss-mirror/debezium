/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.opengauss.process;

/**
 * Description: ProgressStatus enum
 *
 * @author czy
 * @since 2023-06-07
 */
public enum ProgressStatus {
    NOT_MIGRATED(1),
    IN_MIGRATED(2),
    MIGRATED_COMPLETE(3),
    MIGRATED_FAILURE(6);

    private final int code;

    /**
     * enum definition
     *
     * @param code int
     */
    ProgressStatus(int code) {
        this.code = code;
    }

    /**
     * Get enum code
     *
     * @return code
     */
    public int getCode() {
        return code;
    }
}
