/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

package org.full.migration.model.table;

import java.math.BigDecimal;

import lombok.Builder;
import lombok.Getter;

/**
 * SequenceDefinition class defines the sequence definition in Oracle database.
 * 
 * @author wangchao
 * @version 7.0.0
 * @since 2026-04-18
 */
@Builder
@Getter
public class SequenceDefinition {
    private final String sequenceName;
    private final BigDecimal startWith;
    private final BigDecimal incrementBy;
    private final BigDecimal minValue;
    private final BigDecimal maxValue;
    private final boolean isCycle;
    private final Integer cacheSize;
    private final Boolean isOrder;
}
