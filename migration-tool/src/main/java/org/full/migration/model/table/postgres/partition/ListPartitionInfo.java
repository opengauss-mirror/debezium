/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.full.migration.model.table.postgres.partition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Description: list partition table info
 *
 * @since 2025/5/16
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
}
