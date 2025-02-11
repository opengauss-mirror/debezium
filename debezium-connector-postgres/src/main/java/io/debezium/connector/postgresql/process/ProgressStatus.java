/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package io.debezium.connector.postgresql.process;

/**
 * Description: ProgressStatus enum
 *
 * @author jianghongbo
 * @since 2025-02-05
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
