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

import lombok.Data;

import org.full.migration.model.table.SequenceDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;


/**
 * Source2OpenGaussTranslator
 * Translator for converting SQL from source database to openGauss SQL
 *
 * @since 2025-06-06
 */
@Data
public abstract class Source2OpenGaussTranslator implements Source2TargetTranslator {
    private static final Logger LOGGER = LoggerFactory.getLogger(Source2OpenGaussTranslator.class);
    
    @Override
    public String getTargetDatabaseType() {
        return "opengauss";
    }
    
    @Override
    public abstract Optional<String> translate(String sqlIn, boolean isDebug,
                                               boolean isColumnCaseSensitiv);

    public Optional<String> translateIndex(String indexType, boolean isDebug) {
        return Optional.empty();
    }
    
    @Override
    public Optional<String> translatePartitionFunction(String functionCall, boolean isDebug) {
        return Optional.of(functionCall);
    }

    @Override
    public Optional<String> translateView(String name, String viewDDL) {
        return Optional.empty();
    }

    @Override
    public Optional<String> translateFunction(String functionDDL) {
        return Optional.empty();
    }

    @Override
    public Optional<String> translateProcedure(String procedureDDL) {
        return Optional.empty();
    }

    @Override
    public Optional<String> translateTrigger(String triggerDDL) {
        return Optional.empty();
    }

    @Override
    public Optional<String> translateSequence(SequenceDefinition sequence) {
        return Optional.empty();
    }
}
