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

package org.full.migration.model.table;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * subPartitionDefinition
 *
 * @since 2025-06-28
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class OpenGaussPartitionDefinition {
    private long oid;
    private String partitionName;
    private String partColumn;
    private String partstrategy;
    private String partitionType;
    private String boundary;
    private String interval;


    /**
     * isRangePartition
     *
     * @return isRangePartition
     */
    public boolean isRangePartition() {
        return "r".equalsIgnoreCase(partstrategy);
    }

    /**
     * isIntervalPartition
     *
     * @return isIntervalPartition
     */
    public boolean isIntervalPartition() {
        return "i".equalsIgnoreCase(partstrategy);
    }

    /**
     * isListPartition
     *
     * @return isListPartition
     */
    public boolean isListPartition() {
        return "l".equalsIgnoreCase(partstrategy);
    }

    /**
     * isHashPartition
     *
     * @return isHashPartition
     */
    public boolean isHashPartition() {
        return "h".equalsIgnoreCase(partstrategy);
    }
}
