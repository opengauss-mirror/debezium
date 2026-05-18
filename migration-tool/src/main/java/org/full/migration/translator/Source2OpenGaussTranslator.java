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
