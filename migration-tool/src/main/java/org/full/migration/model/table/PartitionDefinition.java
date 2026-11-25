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

import lombok.Data;

import java.util.List;

/**
 * PartitionDefinition
 *
 * @since 2025-04-18
 */
@Data
public class PartitionDefinition {
    private String partitionType;
    private String partitionColumn;
    private boolean isRightRange;
    private List<String> boundaries;
    private int partitionCount;

    /**
     * isRangePartition
     *
     * @return isRangePartition
     */
    public boolean isRangePartition() {
        return "RANGE".equalsIgnoreCase(partitionType);
    }

    /**
     * isListPartition
     *
     * @return isListPartition
     */
    public boolean isListPartition() {
        return "LIST".equalsIgnoreCase(partitionType);
    }

    /**
     * isHashPartition
     *
     * @return isHashPartition
     */
    public boolean isHashPartition() {
        return "HASH".equalsIgnoreCase(partitionType);
    }
}
