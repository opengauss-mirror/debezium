/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.full.migration.validator;

import javax.validation.Constraint;
import javax.validation.Payload;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.annotation.Documented;

/**
 * ValidPluginName
 *
 * @since 2025-06-06
 */
@Documented
@Constraint(validatedBy = PluginNameValidator.class)
@Target({ElementType.FIELD, ElementType.PARAMETER})
@Retention(RetentionPolicy.RUNTIME)
public @interface ValidPluginName {
    String message() default "pluginName is invalid, Please choose between 'pgoutput' and 'wal2json'.\n";

    Class<?>[] groups() default {};

    Class<? extends Payload>[] payload() default {};
}
