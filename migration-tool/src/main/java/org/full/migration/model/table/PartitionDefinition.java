/*
 * Copyright (c) 2025-2025 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *           http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
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
