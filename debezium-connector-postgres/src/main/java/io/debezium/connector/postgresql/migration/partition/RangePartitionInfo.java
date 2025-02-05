/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package io.debezium.connector.postgresql.migration.partition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Description: range partition table info
 *
 * @author jianghongbo
 * @since 2025/1/6
 */
public class RangePartitionInfo extends PartitionInfo {
    private static final Logger LOGGER = LoggerFactory.getLogger(RangePartitionInfo.class);
    private String lowerBound = null;
    private String upperBound = null;

    public RangePartitionInfo() {
        super();
    }

    @Override
    public void setRangeLowerBound(String lowerBound) {
        this.lowerBound = lowerBound;
    }

    @Override
    public void setRangeUpperBound(String upperBound) {
        this.upperBound = upperBound;
    }

    @Override
    public String getRangeLowerBound() {
        return lowerBound;
    }

    @Override
    public String getRangeUpperBound() {
        return upperBound;
    }
}
