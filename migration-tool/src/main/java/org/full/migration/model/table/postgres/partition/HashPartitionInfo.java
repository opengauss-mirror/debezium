/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.full.migration.model.table.postgres.partition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * hash partition table info
 *
 * @since 2025/5/16
 */
public class HashPartitionInfo extends PartitionInfo {
    private static final Logger LOGGER = LoggerFactory.getLogger(HashPartitionInfo.class);
    private String value = "";

    public HashPartitionInfo() {
        super();
    }

    @Override
    public String getHashPartitionValue() {
        return this.value;
    }

    @Override
    public void setHashPartitionValue(String hashPartitionValue) {
        this.value = hashPartitionValue;
    }
}
