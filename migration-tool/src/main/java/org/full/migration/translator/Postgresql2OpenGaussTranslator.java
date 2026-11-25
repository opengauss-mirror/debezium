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

package org.full.migration.translator;

import org.full.migration.model.table.Column;
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

    @Override
    public Optional<String> translateColumnType(String tableName, Column column) {
        return Optional.empty();
    }

    @Override
    public String getSourceDatabaseType() {
        return "postgresql";
    }
}
