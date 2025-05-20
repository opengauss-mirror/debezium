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

package org.full.migration.constants;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.math.BigInteger;

/**
 * Unit
 *
 * @since 2025-04-18
 */
@Getter
@AllArgsConstructor
public enum Unit {
    /**
     * KB
     */
    K(0),
    /**
     * MB
     */
    M(1),
    /**
     * GB
     */
    G(2);
    private static final BigInteger MEMORY_UNIT_BIGINTEGER = BigInteger.valueOf(1024);

    private final int exponent;

    /**
     * calculateSize
     *
     * @param size size
     * @param unit unit
     * @return BigInteger
     */
    public static BigInteger calculateSize(BigInteger size, Unit unit) {
        return size.multiply(MEMORY_UNIT_BIGINTEGER.pow(unit.getExponent()));
    }
}
