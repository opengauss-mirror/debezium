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
