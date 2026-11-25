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

package org.full.migration.validator;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;

/**
 * PluginNameValidator
 *
 * @since 2025-06-10
 */
public class PluginNameValidator implements ConstraintValidator<ValidPluginName, String> {
    @Override
    public boolean isValid(String pluginName, ConstraintValidatorContext constraintValidatorContext) {
        if ("pgoutput".equalsIgnoreCase(pluginName) || "wal2json".equalsIgnoreCase(pluginName)) {
            return true;
        }
        return false;
    }
}
