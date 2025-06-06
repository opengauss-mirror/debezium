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
