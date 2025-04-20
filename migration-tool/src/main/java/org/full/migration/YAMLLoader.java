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

package org.full.migration;

import org.full.migration.model.config.GlobalConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.Set;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;

/**
 * YAMLLoader
 *
 * @since 2025-04-18
 */
public class YAMLLoader {
    private static final Logger LOGGER = LoggerFactory.getLogger(YAMLLoader.class);

    /**
     * loadYamlConfig
     *
     * @param path path
     * @return GlobalConfig
     */
    public static Optional<GlobalConfig> loadYamlConfig(String path) {
        try (InputStream stream = Files.newInputStream(Paths.get(path))) {
            Yaml yaml = new Yaml();
            GlobalConfig globalConfig = yaml.loadAs(stream, GlobalConfig.class);
            Validator validator = Validation.buildDefaultValidatorFactory().getValidator();
            Set<ConstraintViolation<GlobalConfig>> violations = validator.validate(globalConfig);
            if (!violations.isEmpty()) {
                violations.forEach(v -> LOGGER.error("the param '{}' is error, reason: {}, please check and retry.",
                    v.getPropertyPath(), v.getMessage()));
                return Optional.empty();
            }
            return Optional.of(globalConfig);
        } catch (IOException e) {
            LOGGER.error("fail to parse yml config, error message: {}", e.getMessage());
            return Optional.empty();
        }
    }
}
