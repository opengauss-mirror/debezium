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

package org.full.migration.translator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * Postgresql2OpenGaussTranslator
 *
 * @since 2025-05-16
 */
public class Postgresql2OpenGaussTranslator extends Source2OpenGaussTranslator {
    private static final Logger logger = LoggerFactory.getLogger(Postgresql2OpenGaussTranslator.class);

    @Override
    public Optional<String> translate(String sqlIn, boolean isDebug,
                                      boolean isColumnCaseSensitive) {
        return Optional.of(sqlIn);
    }
}
