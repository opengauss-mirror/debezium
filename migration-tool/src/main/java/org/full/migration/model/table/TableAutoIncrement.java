/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

package org.full.migration.model.table;

import lombok.Data;

/**
 * TableAutoIncrement
 *
 * @since 2026-04-18
 */
@Data
public class TableAutoIncrement {
    private String schemaName;
    private String tableName;
    private String columnName;
    private long maxAutoIncrement;
}
