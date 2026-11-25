/*
 * Copyright 2026 Huawei Technologies Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.full.migration.model.table.postgres.partition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Description: range partition table info
 *
 * @since 2025/5/16
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
