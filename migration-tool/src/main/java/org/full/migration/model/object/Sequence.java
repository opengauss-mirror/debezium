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

package org.full.migration.model.object;

import lombok.Data;

/**
 * Sequence
 *
 * @since 2026-02-10
 */
@Data
public class Sequence {
    private String schema;
    private String name;
    private String lastValue;
    private String startValue;
    private long incrementBy;
    private String maxValue;
    private String minValue;
    private long cacheValue;
    private boolean isCycled;
    private boolean isLargeSequence;
    private String ownedTable;
    private String ownedColumn;

    /**
     * Get the owned by table and column.
     *
     * @return the owned by table and column
     */
    public String getOwnedBy() {
        if (ownedTable == null || ownedColumn == null) {
            return "NONE";
        }
        return ownedTable + "." + ownedColumn;
    }
}
