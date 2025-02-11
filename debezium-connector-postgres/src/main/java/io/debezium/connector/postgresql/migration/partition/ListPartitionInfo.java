/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package io.debezium.connector.postgresql.migration.partition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Description: list partition table info
 *
 * @author jianghongbo
 * @since 2025/1/6
 */
public class ListPartitionInfo extends PartitionInfo {
    private static final Logger LOGGER = LoggerFactory.getLogger(ListPartitionInfo.class);
    private String value;

    public ListPartitionInfo() {
        super();
    }

    @Override
    public String getListPartitionValue() {
        return value;
    }

    @Override
    public void setListPartitionValue(String listPartitionValue) {
        this.value = listPartitionValue;
    }

    /**
     * Set value
     *
     * @param value String
     */
    public void setValue(String value) {
        this.value = value;
    }

    /**
     * Get value
     *
     * @return String
     */
    public String getValue() {
        return value;
    }
}
