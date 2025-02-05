/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package io.debezium.connector.postgresql.migration.partition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * hash partition table info
 *
 * @author jianghongbo
 * @since 2025/1/6
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
