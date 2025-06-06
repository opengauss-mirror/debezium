/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.full.migration.model.table.postgres.partition;

import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * partition table info
 *
 * @since 2025/5/16
 */
@Data
public abstract class PartitionInfo {
    private static final Logger LOGGER = LoggerFactory.getLogger(PartitionInfo.class);

    /**
     * RANGE PARTITION
     */
    public static final String RANGE_PARTITION = "RANGE";

    /**
     * HASH PARTITION
     */
    public static final String HASH_PARTITION = "HASH";

    /**
     * LIST PARTITION
     */
    public static final String LIST_PARTITION = "LIST";

    /**
     * empty string
     */
    public static final String EMPTY_STRING = "";

    /**
     * next level partition key
     */
    protected String partitionKey;

    /**
     * partition table name
     */
    protected String partitionTable;

    /**
     * parent tables
     */
    protected String parentTable;

    /**
     * setRangeLowerBound
     *
     * @param lowerBound String
     */
    public void setRangeLowerBound(String lowerBound) {

    }

    /**
     * setRangeUpperBound
     *
     * @param upperBound String
     */
    public void setRangeUpperBound(String upperBound) {

    }

    /**
     * getRangeLowerBound
     *
     *  @return String
     */
    public String getRangeLowerBound() {
        return EMPTY_STRING;
    }

    /**
     * getRangeUpperBound
     *
     * @return String
     */
    public String getRangeUpperBound() {
        return EMPTY_STRING;
    }

    /**
     * getListPartitionValue
     *
     * @return String
     */
    public String getListPartitionValue() {
        return EMPTY_STRING;
    }

    /**
     * setListPartitionValue
     *
     * @param listPartitionValue String
     */
    public void setListPartitionValue(String listPartitionValue) {

    }

    /**
     * getHashPartitionValue
     *
     * @return String
     */
    public String getHashPartitionValue() {
        return EMPTY_STRING;
    }

    /**
     * setHashPartitionValue
     *
     * @param hashPartitionValue String
     */
    public void setHashPartitionValue(String hashPartitionValue) {

    }
}
